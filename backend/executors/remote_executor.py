from mysql.connector import Error
from handers.insert_handler import InsertHandler
from handers.create_handler import CreateTableHandler
from handers.select_handler import SelectHandler
from backend.executors.abstract_executor import AbstractQueryExecutor
from ply.sql_metadata.keywords_lists import QueryType


class RemoteExecutor(AbstractQueryExecutor):
    def __init__(self, conn):
        super().__init__(handler=None)
        self.conn = conn
        self.result = None

    def call(self, query, parser):
        if parser.query_type == QueryType.SELECT:
            self.handler = SelectHandler(query)
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute(self.handler.query)
                self.result = cursor.fetchall()
        if parser.query_type == QueryType.INSERT:
            self.handler = InsertHandler(query)
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute(self.handler.query)
                self.conn.commit()
        # todo: support create database
        if parser.query_type == QueryType.CREATE:
            # first update table meta
            self.handler = CreateTableHandler(query, parser, self.conn.database)
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute(self.handler.query)

