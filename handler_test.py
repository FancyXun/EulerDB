from unittest import TestCase
import requests
import json
import random


class TestPostHandler(TestCase):

    def test_handler_query_limit(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select * from user limit 10'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_create_table(self):
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
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': create_table_sql,
            'encrypt_cols': encrypt_cols}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_insert_table(self):
        for i in range(1000000):
            query = 'insert into user(id_card, name, age, sex, score, nick_name, comments) values ( "' + str(
                random.randint(1000000000000000000, 1000000000000000000000000)) + '","' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '",' + str(random.randint(1, 50)) + ', "' + ''.join(
                random.sample('fm', 1)) + '",' + str(random.randint(60, 100)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '","' + ''.join(
                random.sample('zyxwvutsrqpondgsghgsfgyftuywiecvdbhsbdhgshfdgsgfysgmlkjihgfedcba', 30)) + '")'
            content = {
                'host': '127.0.0.1', 'db': 'points',
                'user': 'root', 'password': 'root',
                'query': query}

            json_data = json.dumps(content)
            requests.post('http://localhost:8888/query', json_data)

    def test_handler_query_where(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select id_card, name, age from user where age = 20'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_where_and(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select id_card, name from user where id_card = "496715970993917044442778" and name = "iezlcpnjws"'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_order_by(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select id_card, name, age from user order by age limit 100'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_max(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select max(age) from user'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_min(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select min(age) from user'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_like(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select id_card, name from user where name like "rax%fpb%"'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_max1(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select max(score) from user limit 10'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_query_min1(self):
        content = {
            'host': '127.0.0.1', 'db': 'points',
            'user': 'root', 'password': 'root',
            'query': 'select min(score) from user limit 10'}

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])
