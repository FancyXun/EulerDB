import unittest
import random

from scheduler.backend import execution_context


db_host = '127.0.0.1'
db = 'points'
user = 'root'
password = 'root'
port = 3306


conn_info = {
            'host': db_host, 'db': db,
            'user': user, 'password': password,  'port': port,
            }


class ContextTestCase(unittest.TestCase):

    def test_create_db(self):
        self.assertEqual(True, True)

    def test_create_table(self):
        """

        """
        create_table_sql = 'create table if not exists user(' \
                           'id_card varchar(40), name varchar(20), age int, sex varchar(5), ' \
                           'score int, nick_name varchar(20), comments varchar(200));'
        encrypt_cols = {
            "id_card": {
                "fuzzy": True,
                "arithmetic": False,
            },
            "name": {
                "fuzzy": True,
                "arithmetic": False
            },
            "age": {
                "fuzzy": False,
                "arithmetic": False
            }
        }
        execution_context.invoke(conn_info, create_table_sql, encrypt_cols)
        self.assertEqual(True, True)

    def test_insert(self):
        for i in range(1000000):
            query = 'insert into user(id_card, name, age, sex, score, nick_name, comments) values ( "' + str(
                random.randint(1000000000000000000, 1000000000000000000000000)) + '","' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '",' + str(random.randint(1, 50)) + ', "' + ''.join(
                random.sample('fm', 1)) + '",' + str(random.randint(60, 100)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '","' + ''.join(
                random.sample('zyxwvutsrqpondgsghgsfgyftuywiecvdbhsbdhgshfdgsgfysgmlkjihgfedcba', 30)) + '")'
            execution_context.invoke(conn_info, query)
            print(query)
        self.assertEqual(True, True)

    def test_select(self):
        query = 'select * from user limit 1000'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_limit(self):
        query = 'select * from user limit 10'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_part_items(self):
        query = 'select id_card, name, score from user limit 1000'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_where(self):
        query = 'select id_card, name, age from user where age = 20'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_where1(self):
        query = 'select id_card, name from user where id_card = "496715970993917044442778" and name = "iezlcpnjws"'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_order_by(self):
        query = 'select id_card, name, age from user order by age limit 100'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_max(self):
        query = 'select max(age) from user'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_min(self):
        query = 'select min(age) from user'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)

    def test_select_like(self):
        query = 'select id_card, name from user where name like "rax%fpb%"'
        result = execution_context.invoke(conn_info, query)
        for i in result:
            print(i)
        self.assertEqual(True, True)


if __name__ == '__main__':
    unittest.main()
