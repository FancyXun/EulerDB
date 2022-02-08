import mysql.connector
from scheduler.backend import execution_context


class ControllerDatabase(object):

    def __init__(self, data):
        self.query_info = data

    def do_query(self):
        connection = mysql.connector.connect(host=self.query_info['host'],
                                             user=self.query_info['user'],
                                             database=self.query_info['db'],
                                             password=self.query_info['password'])
        query = self.query_info['query']
        result = execution_context.invoke(connection, query)

        return {'result': result}
