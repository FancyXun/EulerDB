from abc import ABC

import tornado
from tornado.ioloop import IOLoop
from tornado.web import Application, RequestHandler
import mysql.connector

from src.backend import execution_context


class DBConnection:
    def __init__(self, user, password, db, host='127.0.0.1'):
        self.host = host
        self.user = user
        self.password = password
        self.db = db
        self.__connect__()

    def __connect__(self):
        self.connection = mysql.connector.connect(host=self.host,
                                                  user=self.user,
                                                  database=self.db,
                                                  password=self.password)


# url : 127.0.0.1:8888/?user=root&password=root&db=points&query=select * from user
class IndexHandler(RequestHandler, ABC):
    def get(self):
        user = self.get_query_argument('user', '')
        password = self.get_query_argument('password', '')
        db = self.get_query_argument('db', '')
        query = self.get_query_argument('query', '')
        db_conn = DBConnection(user, password, db)
        execution_context.invoke(db_conn.connection, query)


def make_app():
    return tornado.web.Application([
        (r"/", IndexHandler),
    ])


if __name__ == "__main__":
    app = make_app()
    app.listen(8888)
    tornado.ioloop.IOLoop.current().start()
