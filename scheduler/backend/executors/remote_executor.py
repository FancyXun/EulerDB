import logging
import time
import pandas as pd
import numpy as np

import mysql.connector

from scheduler.handers.rewrite_hander import Rewriter
from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor
from scheduler.compile.keywords_lists import QueryType
from dbutils.pooled_db import PooledDB
from scheduler.schema.metadata import Delta
from scheduler.crypto import encrypt

SQL_TYPES = [
    QueryType.CREATE,
    QueryType.SELECT,
    QueryType.INSERT,
    QueryType.DELETE,
    QueryType.UPDATE,
    QueryType.DROP,
    QueryType.ALTER
]

connection_pool = {}

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('euler_db.log')
handler.setLevel(logging.INFO)
logger.addHandler(handler)


class RemoteExecutor(AbstractQueryExecutor):
    def __init__(self, conn_info):
        super().__init__(handler=None)
        self.conn_info = conn_info
        self.conn = self.connect_db(conn_info)
        self.result = None
        self.encrypted_cols = None
        self.rewriter = None
        self.meta = Delta()
        self.table = None

    @staticmethod
    def connect_db(conn_info):
        if not conn_info:
            return conn_info
        db_key = conn_info['host'] + str(conn_info['port']) + conn_info['db'] + conn_info['user']
        if db_key not in connection_pool:
            connection = PooledDB(
                mysql.connector, 5, host=conn_info['host'], user=conn_info['user'],
                passwd=conn_info['password'], db=conn_info['db'], port=conn_info['port'])
            connection_pool[db_key] = connection
            logging.info("mysql connected from {}".format(db_key))
        else:
            connection = connection_pool[db_key]
        return connection.connection()

    @staticmethod
    def __connect_db(conn_info):
        return mysql.connector.connect(host=conn_info['host'],
                                       user=conn_info['user'],
                                       database=conn_info['db'],
                                       password=conn_info['password'],
                                       port=conn_info['port'])

    @staticmethod
    def __connect_db_pool(conn_info):
        connection = PooledDB(
            mysql.connector, 10, host=conn_info['host'], user=conn_info['user'],
            passwd=conn_info['password'], db=conn_info['db'], port=conn_info['port'])

        return connection.connection()

    def call(self, query, parser, encrypted_cols, use_cursor=True):
        """

        """
        query_type = parser.query_type
        self.encrypted_cols = encrypted_cols
        if query_type not in SQL_TYPES:
            raise NotImplementedError("Not support {} sql type".format(query_type))
        try:
            logger.info("SQL:{}".format(query))
            start_time = time.time()
            enc_query, self.table = self.dispatch(query)
            logger.info("Encrypt SQL:{}".format(enc_query))
            logger.info("Encrypt:{}".format(time.time() - start_time))
            if query_type == QueryType.SELECT:
                # if 'limit' in self.conn_info.keys():
                #     limit = self.conn_info['limit']
                #     if self.rewriter.limit < 0:
                #         enc_query = enc_query + " limit {}".format(limit)
                if not isinstance(self.table, dict):
                    # todo: 长达支持table为dict
                    enc_query = self.inject_procedure(enc_query, use_cursor)
            cursor = self.conn.cursor()
        except Exception as e:
            logger.info(e)
            raise e
        start_time = time.time()
        try:
            cursor.execute(enc_query)
        except Exception as e:
            logger.info(e)
            if query_type == QueryType.CREATE:
                self.meta.delete_delta(self.str_db, self.table['table'])
        if query_type == QueryType.DROP:
            self.meta.delete_delta(self.str_db, self.table['table'])
        elif query_type == QueryType.SELECT:
            self.result = cursor.fetchall()
        else:
            self.conn.commit()
        logger.info("Execute:{}".format(time.time() - start_time))
        self.conn.close()

    def encrypt_sql(self, query):
        try:
            logger.info("SQL:{}".format(query))
            enc_query, _ = self.dispatch(query)
        except Exception as e:
            logger.info(e)
            raise e
        return enc_query

    def batch_insert(self, batch_info):
        columns = batch_info['columns']
        delta, table = self.get_db_meta()
        table = batch_info['table']
        table_info = delta[table]
        new_table = table_info['anonymous']
        columns_info = table_info['columns']
        new_columns = []
        for col in columns:
            if not columns_info[col]['plaintext']:
                for enc, value in columns_info[col]['enc-cols'].items():
                    new_columns.append(value)
            else:
                new_columns.append(col)
        chunk_size = 10 ** 4 * 2
        cursor = self.conn.cursor()
        for chunk in pd.read_csv(batch_info['input'], chunksize=chunk_size, header=None):
            data = np.transpose(chunk.values)
            enc_data = []
            for idx, col in enumerate(columns):
                if not columns_info[col]['plaintext']:
                    for enc, value in columns_info[col]['enc-cols'].items():
                        if enc == "order-preserving":
                            enc_data.append(
                                self.batch_enc(
                                    encrypt.OPECipher(columns_info[col]['key']).encrypt, int, (data[idx])))
                        if enc == "symmetric":
                            enc_data.append(
                                self.batch_enc(
                                    encrypt.AESCipher(columns_info[col]['key']).encrypt, str, (data[idx])))
                        if enc == "fuzzy":
                            enc_data.append(
                                self.batch_enc(
                                    encrypt.FuzzyCipher(columns_info[col]['key']).encrypt, str, (data[idx])))
                else:
                    enc_data.append(data[idx])
            query = "insert into {} (".format(new_table) + ",".join(new_columns) + ") values(" \
                    + ",".join(["%s"] * len(new_columns)) + ")"
            enc_data = np.transpose(np.asarray(enc_data)).tolist()
            for idx, i in enumerate(enc_data):
                cursor.execute(query, tuple(i))
            self.conn.commit()
        self.conn.close()

    @staticmethod
    def batch_enc(func, input_type, data):
        enc_data = []
        for i in data:
            enc_data.append(func(input_type(i)))
        return enc_data

    def dispatch(self, query):
        self.rewriter = Rewriter(self.str_db, self.encrypted_cols)
        return self.rewriter.rewrite_query(query)

    def get_select_columns(self):
        return self.rewriter.select.select_columns

    def get_select_types(self):
        return self.rewriter.select.select_types

    def inject_procedure(self, enc_query, use_cursor=True):
        # todo: string match is workaround
        if ' SUM(' not in enc_query and ' AVG(' not in enc_query:
            return enc_query
        sum_feature_name_list, avg_feature_name_list, need_paillier_procedure, table_name, enc_query = \
            Delta.get_paillier_procedure_info(enc_query, self.str_db, self.table)
        if not need_paillier_procedure:
            return enc_query
        for feature in set(sum_feature_name_list+avg_feature_name_list):
            Delta.set_sum_feature(self.conn.cursor(), feature, table_name)
        if avg_feature_name_list:
            Delta.set_total_feature_num(self.conn.cursor(), table_name)
        for feature in sum_feature_name_list:
            # Delta.create_paillier_sum_procedure(self.conn.cursor(), feature, table_name, use_cursor)
            enc_query = Delta.modify_sum_query(enc_query, feature[0], use_cursor)
        for feature in avg_feature_name_list:
            # Delta.create_paillier_sum_procedure(self.conn.cursor(), feature, table_name, use_cursor)
            enc_query = Delta.modify_avg_query(enc_query, feature[0], use_cursor, 'TotalFeature')
        enc_query = enc_query + ' limit 1'
        return enc_query

    def get_db_meta(self):
        return Delta().meta[self.str_db], self.table

    @property
    def str_db(self):
        # return str(self.conn_info['host']) + ":" + str(self.conn_info['port']) + "/" + str(self.conn_info['db'])
        return str(self.conn_info['db'])


class ConvertExecutor(AbstractQueryExecutor):
    def __init__(self):
        self.str_db = None
        self.rewriter = None
        self.encrypted_cols = None

    def dispatch(self, db, _sql):
        self.rewriter = Rewriter(db)
        self.str_db = db
        enc_query, self.table = self._dispatch(_sql)
        return enc_query, self.table

    def _dispatch(self, query):
        self.rewriter = Rewriter(self.str_db, self.encrypted_cols)
        return self.rewriter.rewrite_query(query)

    def get_select_columns(self):
        return self.rewriter.select.select_columns

    def get_select_types(self):
        return self.rewriter.select.select_types

    def get_db_meta(self):
        return Delta().meta[self.str_db], self.table


if __name__ == '__main__':
    import pprint
    sql = [
           # 'select * from table_fe88068167757a7800ad3435283e7102;',
           # 'select * from table_fe88068167757a7800ad3435283e7102 join table_fe88068167757a7800ad3435283e7102;',
           #  'select max(age) from {}'.format('table_10cca3e8d3ba7459c1c08fe7ec212f21'),
           # 'select * from (select * from table_fe88068167757a7800ad3435283e7102) a;',
           #  'select age from table_fe88068167757a7800ad3435283e7102',
            'select a.age from (select * from table_fe88068167757a7800ad3435283e7102)  as a'
           # 'select * from (select * from table_fe88068167757a7800ad3435283e7102) a, (select * from table_fe88068167757a7800ad3435283e7102) b;'

           ]
    query_info = {'host': '127.0.0.1', 'db': 'points', 'user': 'root', 'password': 'rootroot', 'port': 3306}
    for i in sql:
        print(i)
        query_info['query'] = i
        executor = RemoteExecutor(conn_info=query_info)
        res = executor.dispatch(i)
        print(executor.rewriter.select.select_columns)
        print(executor.rewriter.select.select_types)
        print(executor.rewriter.select.select_state)
        print(res[0])
        # print(res[1])

