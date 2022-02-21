from unittest import TestCase
import requests
import json
import random
import unittest

db_host = '127.0.0.1'
db = 'points'
user = 'root'
password = 'root'
port = 3306

content = {
    'host': db_host, 'db': db,
    'user': user, 'password': password,
    'port': port
}


def request_api():
    return


sql_list = {
    'where':
        ['select * from test limit 5',
         'select id_card, name, age from test where age = 20 limit 5',
         'select id_card, name, age, score  from test where score = 60 limit 5',
         'select id_card, name, age from test where age > 40 limit 100',
         'select id_card, name, age from test where age < 10 limit 100',
         'select id_card, name, age from test where age <= 10 limit 100',
         ],
    'order_by_min_max':
        ['select * from test where id_card = "496715970993917044442778" and name = "iezlcpnjws"',
         'select id_card, name, age from test order by age limit 5',
         'select distinct id_card, name, age from test limit 5',
         'select distinct age, name, id_card from test limit 1000',
         'select max(age), min(age) from test',
         'select id_card, name from test WHERE age = 30 limit 100',
         'select max(score), min(score) from test '
         ],
    'like':
        ['select id_card, name from test where name like "rax%fpb%" limit 5'],
    'count':
        ['select count(*) from test'],
    'delete':
        ['DELETE FROM test WHERE id_card = "496715970993917044442778" and name = "iezlcpnjws" '],
    'alter':
        ['ALTER TABLE test ADD Birthday date'],
    'update':
        ['UPDATE test set id_card = "496715970993917044442778" , name = "iezlcpnjws" WHERE age = 30']

}


class TestPostHandler(TestCase):

    def test_handler_create_table(self):
        create_table_sql = 'create table if not exists test(' \
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
                "arithmetic": False
            }
        }
        content['query'] = create_table_sql
        content['encrypted_columns'] = encrypted_columns

        json_data = json.dumps(content)
        resp = requests.post('http://localhost:8888/query', json_data)
        print(resp.json()['result'])

    def test_handler_insert_table(self):
        for i in range(10000):
            query = 'insert into test(id_card, name, age, sex, score, nick_name, comments) values ( "' + str(
                random.randint(1000000000000000000, 1000000000000000000000000)) + '","' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '",' + str(random.randint(1, 50)) + ', "' + ''.join(
                random.sample('fm', 1)) + '",' + str(random.randint(60, 100)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '","' + ''.join(
                random.sample('zyxwvutsrqpondgsghgsfgyftuywiecvdbhsbdhgshfdgsgfysgmlkjihgfedcba', 30)) + '")'
            content['query'] = query
            json_data = json.dumps(content)
            requests.post('http://localhost:8888/query', json_data)

    def test_handler_sql(self):
        for key, value in sql_list.items():
            print("---{}...".format(key))
            for v in value:
                content['query'] = v
                json_data = json.dumps(content)
                try:
                    resp = requests.post('http://localhost:8888/query', json_data)
                    if 'result' in resp.json():
                        print(resp.json()['result'])
                except Exception as e:
                    print(e)
                    continue


class TestRewriterHandler(TestCase):

    def test_select_rewrite(self):
        sql = {
            'query': 'select id_card, name, age from test where age = 20 limit 5',
            'db': db
        }
        json_data = json.dumps(sql)
        resp = requests.post('http://localhost:8888/rewrite_query', json_data)
        print(resp.text)


if __name__ == "__main__":
    unittest.main()
