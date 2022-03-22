import logging

import mysql.connector

from scheduler.handers.rewrite_hander import Rewriter
from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor
from scheduler.compile.keywords_lists import QueryType
from dbutils.pooled_db import PooledDB
from scheduler.schema.metadata import Delta

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
            logging.info("mysql connected from {}" .format(db_key))
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
            enc_query, self.table = self.dispatch(query, self.conn_info['db'], self.encrypted_cols)
            if query_type == QueryType.SELECT and 'limit' in self.conn_info.keys():
                enc_query = enc_query + self.conn_info['limit']
            logging.info("Encrypted sql is {}".format(enc_query))
            cursor = self.conn.cursor()
        except Exception as e:
            logging.info(e)
            raise e
        try:
            enc_query = self.inject_procedure(cursor, enc_query, use_cursor)
            cursor.execute(enc_query)
        except Exception as e:
            logging.info(e)
            if query_type == QueryType.CREATE:
                self.meta.delete_delta(self.conn_info['db'], self.table['table'])
        if query_type == QueryType.DROP:
            self.meta.delete_delta(self.conn_info['db'], self.table['table'])
        elif query_type == QueryType.SELECT:
            self.result = cursor.fetchall()
        else:
            self.conn.commit()
        self.conn.close()

    def dispatch(self, query, db, enc_cols):
        self.rewriter = Rewriter(db, enc_cols)
        return self.rewriter.rewrite_query(query)

    def get_sql_columns(self):
        return self.rewriter.select.select_columns

    def inject_procedure(self, cursor, enc_query, use_cursor=True):
        if 'SUM' not in enc_query and 'AVG' not in enc_query:
            return enc_query
        sum_feature_name_list, avg_feature_name_list, need_paillier_procedure, table_name, enc_query = \
            Delta.get_paillier_procedure_info(enc_query, self.conn_info['db'], self.table)
        if not need_paillier_procedure:
            return enc_query
        for feature in sum_feature_name_list:
            Delta.create_paillier_sum_procedure(cursor, feature, table_name, use_cursor)
            enc_query = Delta.modify_sum_query(enc_query, feature[0], use_cursor)
        for feature in avg_feature_name_list:
            Delta.create_paillier_sum_procedure(cursor, feature, table_name, use_cursor)
            enc_query = Delta.modify_avg_query(enc_query, feature[0], use_cursor)
        enc_query = enc_query + ' limit 1'
        return enc_query

    def get_db_meta(self):
        return Delta().meta[self.conn_info['db']], self.table