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


table = 'test'


sql_list = {
    'where':
        ['select * from {} limit 5'.format(table),
         'select id_card, name, age from {} where age = 20 limit 5'.format(table),
         'select id_card, name, age, score  from {} where score = 60 limit 5'.format(table),
         'select id_card, name, age from {} where age > 40 limit 100'.format(table),
         'select id_card, name, age from {} where age < 10 limit 100'.format(table),
         'select id_card, name, age from {} where age <= 10 limit 100'.format(table),
         'select id_card, name, age, score from {} where score > 70 limit 100'.format(table),
         ],
    'order_by_min_max':
        ['select * from {} where id_card = "310310310" and name = "post"'.format(table),
         'select * from {} where id_card = "310310311" or name = "always"'.format(table),
         'select id_card, name, age from {} order by age limit 5'.format(table),
         'select distinct id_card, name, age from {} limit 5'.format(table),
         'select distinct age, name, id_card from {} limit 1000'.format(table),
         'select max(age), min(age) from {}'.format(table),
         'select id_card, name from {} WHERE age = 30 limit 100'.format(table),
         'select max(score), min(score) from {} '.format(table),
         'select sum(score), avg(score) from {} '.format(table),
         'select avg(age), sum(age), count(*) from {} '.format(table),
         'select sum(age) from {} '.format(table),
         'select max(score), min(age) from {} '.format(table)
         ],
    'like':
        ['select id_card, name from {} where name like "%con%" limit 5'.format(table),
         'select id_card, name, nick_name from {} where nick_name like "con%ent%" limit 5'.format(table), ],
    'count':
        ['select count(*) from {}'.format(table)],
    'delete':
        ['DELETE FROM {} WHERE id_card = "310310319" and name = "content" '.format(table)],
    # 'alter':
    #     ['ALTER TABLE {} ADD Birthday date'.format(table)],
    'update':
        ['UPDATE {} set id_card = "310310319" , name = "content" WHERE age = 30'.format(table)],
    # 'alter key':
    #     ['ALTER TABLE {} DROP FOREIGN KEY fk_emp_dept1;'.format(table),
    #      'ALTER TABLE {} DROP PRIMARY KEY;'.format(table)]
}


class TestPostHandler(TestCase):

    def test_handler_create_table(self):
        create_table_sql = 'create table if not exists {}(' \
                           'id_card varchar(40), name varchar(20), age int, sex varchar(5), ' \
                           'score int, nick_name varchar(20), comments varchar(200), weight float, edu char(5), height double);'.format(table)

        encrypted_columns = {
            "id_card": {
                "fuzzy": True,
                "key": "abcdefgpoints"
            },
            "name": {
                "fuzzy": True,
                "key": "abcdefghijklmn"
            },
            "age": {
                "fuzzy": False,
                "key": "abcdefgopqrst",
                "arithmetic": True,
                "homomorphic_key": [787659527, 1023624989],
            },
            "weight": {
                "fuzzy": False,
                "key": "abcdefgopqrst"
            },
            "edu": {
                "fuzzy": True,
                "key": "abcdefghisdfn"
            }
        }
        content['query'] = create_table_sql
        content['encrypted_columns'] = encrypted_columns

        json_data = json.dumps(content)
        print(json_data)
        resp = requests.post('http://localhost:8888/query', json_data)
        content.pop('encrypted_columns')
        print(resp.json()['result'])

    # def test_drop_table_key(self):
    #     for sql in sql_list['alter key']:
    #         content['query'] = sql
    #         json_data = json.dumps(content)
    #         requests.post('http://localhost:8888/query', json_data)

    def test_drop_table(self):
        sql = 'drop table {}'.format(table)
        content['query'] = sql
        json_data = json.dumps(content)
        requests.post('http://localhost:8888/query', json_data)

    def test_handler_insert_table(self):
        data = ['content', 'random', 'test', 'always', 'users', 'json', 'localhost', 'value', 'handler', 'continue',
                'requests', 'post', '夯实', '杭州市', '杭州市大华', '光之树科技', '中国', '中', '上海', '上海市浦东新区疫情地图',
                '阿里巴巴', '百度', '高德地图', '名次', '中国', '中', '上海', '上海市浦东新区疫情地图']
        for i in range(100):
            query = 'insert into {}(id_card, name, age, sex, score, nick_name, comments, weight, edu, height) values ( "' + \
                    str(310310310 + i) + \
                    '","' + ''.join(data[random.randint(0, 20)]) + '",' + str(random.randint(1, 100)) + ', "' + ''.join(
                random.sample('fm', 1)) + '",' + str(random.randint(60, 100)) + ',"' + ''.join(
                data[random.randint(0, 10)]) + '","' + ''.join(data[random.randint(0, 10)]) + '","' + str(10*random.random()) + \
                    '","' + 'colle' + '","' + str(10*random.random()) + '")'
            content['query'] = query.format(table)
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
