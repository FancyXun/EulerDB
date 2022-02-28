import unittest
import requests
import json

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


table = 'test'
table1 = 'test1'

sql_list = {
    "tables":
    ['SELECT {}.id_card, {}.age, {}.age, {}.score '
     'FROM {}, {} WHERE {}.age = {}.age limit 10'.format(table, table, table1, table1, table, table1, table, table1)],
    "join":
    ['SELECT {}.id_card, {}.age, {}.age FROM {} INNER JOIN {} ON {}.age = {}.age limit 10'
     .format(table, table, table1, table, table1, table, table1),
     'SELECT {}.id_card, {}.age, {}.age FROM {} INNER JOIN {} ON {}.age = {}.score limit 10'
     .format(table, table, table1, table, table1, table, table1),
     'SELECT {}.id_card, {}.age, {}.age FROM {} JOIN {} ON {}.age = {}.age limit 10'
     .format(table, table, table1, table, table1, table, table1),
     'SELECT {}.id_card, {}.age, {}.age FROM {} LEFT JOIN {} ON {}.age = {}.age limit 10'
     .format(table, table, table1, table, table1, table, table1),
     'SELECT {}.id_card, {}.score, {}.score FROM {} LEFT JOIN {} ON {}.score = {}.score limit 10'
     .format(table, table, table1, table, table1, table, table1),
     'SELECT {}.id_card, {}.score, {}.score FROM {} RIGHT JOIN {} ON {}.score = {}.score limit 10'
     .format(table, table, table1, table, table1, table, table1),
     'SELECT {}.id_card, {}.age, {}.age FROM {} RIGHT JOIN {} ON {}.age = {}.age limit 10'
     .format(table, table, table1, table, table1, table, table1),
     ]

}


class MyTestCase(unittest.TestCase):
    def test_sql(self):
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


if __name__ == '__main__':
    unittest.main()
