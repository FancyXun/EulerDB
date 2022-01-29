import unittest
import random

import mysql.connector

from scheduler.backend import execution_context


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


db_conn = DBConnection("root", "root", "points")


class ContextTestCase(unittest.TestCase):

    def test_create_db(self):
        self.assertEqual(True, True)

    def test_create_table(self):
        # create table
        create_table_sql = 'create table if not exists user(' \
                           'id int, name varchar(20), age int, sex varchar(5), ' \
                           'score int, nick_name varchar(20), comments varchar(200));'
        execution_context.invoke(db_conn.connection, create_table_sql)
        self.assertEqual(True, True)

    def test_insert(self):
        for i in range(1000000):
            query = 'insert into user(id, name, age, sex, score, nick_name, comments) values (' + str(
                random.randint(1000, 10000)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 5)) + '",' + str(random.randint(1, 50)) + ', "' + ''.join(
                random.sample('fm', 1)) + '",' + str(random.randint(60, 100)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '","' + ''.join(
                random.sample('zyxwvutsrqpondgsghgsfgyftuywiecvdbhsbdhgshfdgsgfysgmlkjihgfedcba', 30)) + '")'
            execution_context.invoke(db_conn.connection, query)
            print(query)
        self.assertEqual(True, True)

    def test_select(self):
        query = 'select * from user '
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_limit(self):
        query = 'select * from user limit 10'
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_part_items(self):
        query = 'select id, name from user'
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_where(self):
        query = 'select id, name from user where id = 2090'
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_where1(self):
        query = 'select id, name from user where id = 2090 and name = "gbmyx"'
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_order_by(self):
        query = 'select id, name, score from user order by score limit 100'
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_max(self):
        query = 'select max(id) from user'
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_min(self):
        query = 'select min(id) from user'
        result = execution_context.invoke(db_conn.connection, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
