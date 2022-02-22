import re
import mysql.connector
from mysql.connector import Error

from scheduler.handers.rewrite_hander import Rewriter
from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor
from scheduler.compile.keywords_lists import QueryType
from scheduler.schema.metadata import Delta
from dbutils.pooled_db import PooledDB

SQL_TYPES = [
    QueryType.CREATE, QueryType.SELECT, QueryType.INSERT, QueryType.DELETE, QueryType.UPDATE
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

    @staticmethod
    def connect_db(conn_info):
        if not conn_info:
            return conn_info
        db_key = conn_info['host'] + str(conn_info['port']) + conn_info['db'] + conn_info['user']
        if db_key not in connection_pool:
            connection = PooledDB(
                mysql.connector, 5, host=conn_info['host'], user=conn_info['user'],
                passwd=conn_info['password'], db=conn_info['db'], port=conn_info['port'])
            # connection = mysql.connector.connect(host=conn_info['host'],
            #                                      user=conn_info['user'],
            #                                      database=conn_info['db'],
            #                                      password=conn_info['password'],
            #                                      port=conn_info['port'])
            connection_pool[db_key] = connection
            print("mysql connected from {}" .format(db_key))
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

    def call(self, query, parser, encrypted_cols, mini=False):
        """

        """
        query_type = parser.query_type
        self.encrypted_cols = encrypted_cols
        if query_type not in SQL_TYPES:
            raise NotImplementedError("Not support {} sql type".format(query_type))
        if self.encrypted_cols:
            if parser.query_type == QueryType.CREATE:
                enc_query = self.dispatch(query, self.conn_info['db'], self.encrypted_cols)
                try:
                    cursor = self.conn.cursor()
                    cursor.execute(enc_query)
                    Delta().save_delta()
                except Error as e:
                    print("Error while connecting to MySQL", e)
                    # todo:delete tabel meta
        else:
            if parser.query_type == QueryType.SELECT:
                enc_query = self.dispatch(query, self.conn_info['db'], self.encrypted_cols)
                cursor = self.conn.cursor()
                enc_query = self.inject_procedure(cursor, enc_query, mini)
                cursor.execute(enc_query)
                self.result = cursor.fetchall()
            else:
                enc_query = self.dispatch(query, self.conn_info['db'], self.encrypted_cols)
                cursor = self.conn.cursor()
                cursor.execute(enc_query)
                self.conn.commit()
        self.conn.close()

    def dispatch(self, query, db, enc_cols):
        self.rewriter = Rewriter(db, enc_cols)
        return self.rewriter.rewrite_query(query)

    def get_sql_columns(self):
        return self.rewriter.select_columns

    @staticmethod
    def inject_procedure(cursor, enc_query, mini=False):
        if 'SUM' not in enc_query and 'AVG' not in enc_query:
            return enc_query
        sum_feature_name_list, avg_feature_name_list, need_paillier_procedure = \
            Delta.get_paillier_procedure_info()
        table_name = enc_query[re.search('FROM ', enc_query).span()[1]:]
        if not need_paillier_procedure:
            return enc_query
        for feature_name in sum_feature_name_list:
            Delta.create_paillier_sum_procedure(cursor, feature_name, table_name, mini)
            enc_query = Delta.modify_sum_query(enc_query, feature_name, mini)
        for feature_name in avg_feature_name_list:
            Delta.create_paillier_sum_procedure(cursor, feature_name, table_name, mini)
            enc_query = Delta.modify_avg_query(enc_query, feature_name, mini)
        return enc_query