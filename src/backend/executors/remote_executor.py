from mysql.connector import Error
from src.handers.insert_handler import InsertHandler
from src.handers.create_handler import CreateTableHandler
from src.handers.select_handler import SelectHandler
from src.backend.executors.abstract_executor import AbstractQueryExecutor
from src.compile.keywords_lists import QueryType
from src.schema.metadata import Delta


class RemoteExecutor(AbstractQueryExecutor):
    def __init__(self, conn):
        super().__init__(handler=None)
        self.conn = conn
        self.result = None

    def call(self, query, parser):
        if parser.query_type == QueryType.SELECT:
            self.handler = SelectHandler(query, parser, self.conn.database)
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute(self.handler.query)
                self.result = cursor.fetchall()
        if parser.query_type == QueryType.INSERT:
            self.handler = InsertHandler(query, parser, self.conn.database)
            if self.conn.is_connected():
                cursor = self.conn.cursor()
                cursor.execute(self.handler.query)
                self.conn.commit()
        # todo: support create database
        if parser.query_type == QueryType.CREATE:
            # first update table meta
            self.handler = CreateTableHandler(query, parser, self.conn.database)
            if self.conn.is_connected():
                try:
                    cursor = self.conn.cursor()
                    cursor.execute(self.handler.query)
                    Delta().save_delta()
                except Error as e:
                    print("Error while connecting to MySQL", e)
                    # delete metadata

