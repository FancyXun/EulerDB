from unittest import TestCase
import requests
import json
import random
import unittest

db_host = '127.0.0.1'
db = 'points'
user = 'root'
password = 'mysqlxia123'
port = 3306


class TestPostHandler(TestCase):

    def test_handler_create_table(self):
        create_table_sql = 'create table if not exists user_test(' \
                           'id_card varchar(40), name varchar(20), age int, sex varchar(5), ' \
                           'score int, nick_name varchar(20), comments varchar(200));'

        encrypted_columns = {
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
                "arithmetic": True
            }
        }
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password,
            'port': port,
            'query': create_table_sql,
            'encrypted_columns': encrypted_columns}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_limit(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password,
            'port': port,
            'query': 'select * from user_test limit 5'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_insert_table(self):
        for i in range(10):
            query = 'insert into user_test(id_card, name, age, sex, score, nick_name, comments) values ( "' + str(
                random.randint(1000000000000000000, 1000000000000000000000000)) + '","' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '",' + str(random.randint(1, 50)) + ', "' + ''.join(
                random.sample('fm', 1)) + '",' + str(random.randint(60, 100)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '","' + ''.join(
                random.sample('zyxwvutsrqpondgsghgsfgyftuywiecvdbhsbdhgshfdgsgfysgmlkjihgfedcba', 30)) + '")'

            content = {
                'host': db_host, 'db': db,
                'user': user, 'password': password,
                'port': port,
                'query': query}

            json_data = json.dumps(content)
            requests.post('http://localhost:8888/query', json_data)

    def test_insert_one_row(self):
        # fixme
        query = "insert user_test(id_card, name, age, sex, score, nick_name, comments) " \
                "values (" \
                "'496715970993917044442778', 'iezlcpnjws', 20, 'm', 92, " \
                "'duriolzvwj', 'sswgyyydewbxsgzsduggstfhnrgsvj')"
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': query}

        json_data = json.dumps(content)
        requests.post('http://localhost:8888/query', json_data)

    def test_handler_query_where(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name, age from user_test where age = 20 limit 5'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_greater(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name, age from user_test where age > 40 limit 100'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_greater_equal(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name, age from user_test where age >= 40 limit 100'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_less(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name, age from user_test where age < 10 limit 100'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_less_equal(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name, age from user_test where age <= 10 limit 100'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_where_and(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select * from user_test where id_card = "496715970993917044442778" and name = "iezlcpnjws"'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_order_by(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name, age from user_test order by age limit 5'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_distinct(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select distinct id_card, name, age from user_test limit 5'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_distinct1(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select distinct age, name, id_card from user_test limit 1000'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_max(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select max(age) from user_test'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_min(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select min(age) from user_test'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_like(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name from user_test where name like "rax%fpb%" limit 5'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_max1(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select max(score) from user_test '}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_min1(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select min(score), sum(score), avg(score), sum(age), avg(age), count(*) from user_test'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_alter_table(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'ALTER TABLE user_test ADD Birthday date'}
        json_data = json.dumps(content)
        requests.post('http://localhost:8888/query', json_data)

    def test_handler_count(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select count(*) from user_test'}
        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_delete_row(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'DELETE FROM user_test WHERE id_card = "496715970993917044442778" and name = "iezlcpnjws" '}
        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_update(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'UPDATE user_test set id_card = "496715970993917044442778" , name = "iezlcpnjws" '
                     'WHERE age = 30 '}
        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_after_update_select(self):
        content = {
            'host': db_host, 'db': db,
            'user': user, 'password': password, 'port': port,
            'query': 'select id_card, name from user_test '
                     'WHERE age = 30 limit 100'}
        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])


class TestRewriterHandler(TestCase):

    def test_select_rewrite(self):
        content = {
            'query': 'select id_card, name, age from user_test where age = 20 limit 5',
            'db': db
        }
        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/rewrite_query', json_data)
        print(resp.text)


if __name__ == "__main__":
    unittest.main()
