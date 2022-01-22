from mysql.connector import Error
from handers.insert_hander import InsertHandler
from handers.create_hander import CreateTableHandler
from handers.select_handler import SelectHandler
from backend.executors.abstract_executor import AbstractQueryExecutor


class RemoteExecutor(AbstractQueryExecutor):
    def __init__(self, conn):
        super().__init__(handler=None)
        self.conn = conn

    def call(self, query, sql_command='SQL_SELECT'):
        if sql_command == 'SQL_SELECT':
            self.handler= SelectHandler(query)
            try:
                if self.conn.is_connected():
                    cursor = self.conn.cursor()
                    cursor.execute(self.handler.query)
                    result = cursor.fetchall()
                    return result
            except Error as e:
                print("Error while connecting to MySQL", e)
            finally:
                if self.conn.is_connected():
                    cursor.close()
                    self.conn.close()
