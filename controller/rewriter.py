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
        encrypted_cols = None
        if "encrypt_cols" in self.query_info.keys():
            encrypted_cols = self.query_info['encrypt_cols']
        result = execution_context.invoke(connection, query, encrypted_cols, columns_info=True)
        return {'result': result[0],
                'columns': result[1]}


class ControllerRewriter(object):

    def __init__(self, data):
        self.query_info = data

    def do_rewrite(self):
        query = self.query_info['query']
        if isinstance(query, list):
            query = query[0].decode("utf-8")
        db = self.query_info['db']
        if isinstance(db, list):
            db = db[0].decode("utf-8")
        encrypted_cols = None
        if "encrypt_cols" in self.query_info.keys():
            encrypted_cols = self.query_info['encrypt_cols']
        return execution_context.rewrite(query, db, encrypted_cols)
