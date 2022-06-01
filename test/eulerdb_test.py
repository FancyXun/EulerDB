import hashlib
import time
import json
import requests
import mysql.connector
import random
from unittest import TestCase
from decimal import Decimal


def AllRange(sentences, words, start, end):
    if start == end:
        sentences.append("".join(words))
    for m in range(start, end + 1):
        words[m], words[start] = words[start], words[m]
        AllRange(sentences, words, start + 1, end)
        words[m], words[start] = words[start], words[m]


def generate_words():
    sentences = []
    words = ['杭州', '光之树', '大华', '中国', '集团', '公司', '上海', '腾讯', '有限', '因为', '研究生', '西交大']
    words = [words[random.randint(0, len(words) - 1)] for _ in range(8)]
    AllRange(sentences, words, 0, 5)
    words = ['张江', '阿里', '百度', '科技', '北京', '深圳', '广州', '字节跳动', '云计算方向', '研究院', '天气', '大合唱']
    words = [words[random.randint(0, len(words) - 1)] for _ in range(8)]
    AllRange(sentences, words, 0, 5)
    words = ['浙江', '疫情', '苏州', '南京', '成都', '四川', '重庆', '陕西', '灯光',
             '解决人口问题', '进行婚育补贴帮助', '人口生产任务', '高消费']
    words = [words[random.randint(0, len(words) - 1)] for _ in range(8)]
    AllRange(sentences, words, 0, 5)
    return sentences


def get_data(num=float("inf")):
    id_card = []
    sentences = generate_words()
    age = []
    score = []
    edu = []
    comments = generate_words()
    weight = []
    height = []
    for i in range(len(sentences)):
        if i > num:
            break
        id_card.append(str(310310310 + i))
        age.append(str(random.randint(1, 100)))
        score.append(str(random.randint(60, 100)))
        weight.append(str(round(random.random(), 4)))
        edu.append(random.choice(['post', 'bach', 'high']))
        height.append(str(round(300*random.random(), 12)))

    return [id_card, sentences, age, score, comments, weight, edu, height]


def req(_db_info, _cx, sql, sql_local=None):
    _db_info['query'] = sql
    requests.post('http://localhost:8888/query', json.dumps(_db_info))
    cu = _cx.cursor()
    if sql_local:
        print(sql_local)
        cu.execute(sql_local)
    else:
        cu.execute(sql)
    _cx.commit()
    cu.close()


def req_select(_db_info, _cx, sql):
    _db_info['query'] = sql
    result = requests.post('http://localhost:8888/query', json.dumps(_db_info))
    cu = _cx.cursor()
    cu.execute(sql)
    mysql_result = cu.fetchall()
    cu.close()
    return result.json()['result'], mysql_result


class E2ETest(TestCase):

    @staticmethod
    def create_table(_db_info, _cx, refer_table="table_spare"):
        _table = "table_" + hashlib.md5(str(time.process_time()).encode('utf-8')).hexdigest()
        sql = 'create table if not exists {}(' \
              'id_card varchar(100), ' \
              'sentences varchar(100), ' \
              'age int, ' \
              'score int, ' \
              'comments varchar(100), ' \
              'weight float, ' \
              'edu char(4), ' \
              'height double);'.format(_table)

        encrypted_columns = {
            "id_card": {
                "fuzzy": True,
                "key": "abcdefgpoints"
            },
            "sentences": {
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
            },
            "height": {
                "fuzzy": False,
                "key": "abcdefgopqrst"
            }
        }
        _db_info['encrypted_columns'] = encrypted_columns
        print("create table {}".format(_table))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-"*100)
        _db_info.pop('encrypted_columns')
        return _table

    @staticmethod
    def drop_table(_db_info, _cx, _table):
        sql = 'drop table {}'.format(_table)
        print("drop table {}".format(_table))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def add_primary_key(_db_info, _cx, _table, column_str='age'):
        sql = 'alter table {} add primary key({});'.format(_table, column_str)
        print("alter table {} add primary key({})".format(_table, column_str))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def drop_primary_key(_db_info, _cx, _table):
        sql = 'alter table {} drop primary key;'.format(_table)
        print("alter table {} drop primary key".format(_table))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def add_foreign_key(_db_info, _cx, _table, refer_table='table_spare', column_str='age'):
        def create_spare_table(_db_info, _cx, _table="table_spare"):
            sql = 'create table if not exists {}(' \
                  'age int primary key, ' \
                  'score int, ' \
                  'comments varchar(100));'.format(_table)

            encrypted_columns = {
                "age": {
                    "fuzzy": False,
                    "key": "abcdefgopqrst",
                    "arithmetic": True,
                    "homomorphic_key": [787659527, 1023624989],
                }
            }
            _db_info['encrypted_columns'] = encrypted_columns
            print("create table {}".format(_table))
            print("--" + "\n" + "--")
            req(_db_info, _cx, sql)
            print("finished".format(_table))
            print("-" * 100)
            _db_info.pop('encrypted_columns')
            return _table

        refer_table = create_spare_table(_db_info, _cx, refer_table)
        sql = 'alter table {} add constraint {}_fk_test foreign key({}) references {}({});'.format(_table, refer_table, column_str, refer_table, column_str)
        sql_local = 'alter table {} add constraint {}_local_fk_test foreign key({}) references {}({});'.format(_table, refer_table, column_str, refer_table, column_str)
        print("alter table {} add foreign key {}_fk_test".format(_table, refer_table))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql, sql_local)
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def drop_foreign_key(_db_info, _cx, _table, refer_table="table_spare"):
        sql = 'alter table {} drop foreign key {}_fk_test;'.format(_table, refer_table)
        sql_local = 'alter table {} drop foreign key {}_local_fk_test;'.format(_table, refer_table)
        print("alter table {} drop foreign key {}_fk_test".format(_table, refer_table))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql, sql_local)
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def insert_sql(_db_info, _cx, _table, num=float("inf")):
        insert_data = get_data(num)
        print("insert table {}".format(_table))
        print("--" + "\n" + "--")
        for i in zip(*insert_data):
            sql = 'insert into {}(' \
                  'id_card, ' \
                  'sentences, ' \
                  'age, ' \
                  'score, ' \
                  'comments, ' \
                  'weight, ' \
                  'edu, ' \
                  'height) values ( "' + \
                  i[0] + '", "' + i[1] + '", ' + i[2] + ', ' + i[3] + ', "' + i[4] + '", ' + i[5] + ', "' + i[6] + \
                  '", ' + i[7] + ' )'
            sql = sql.format(_table)
            req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-" * 100)

    def test_select_sql(self, _db_info, _cx, _table):
        print("select table {}".format(_table))
        print("--" + "\n" + "--")
        select = ['select count(*) from {}'.format(_table),
                  'select * from {} limit 5'.format(_table),
                  'select id_card, sentences, age, score, comments from {} limit 4'.format(_table),
                  'select weight from {} limit 4'.format(_table),
                  'select height from {} limit 4'.format(_table),
                  'select edu from {} limit 4'.format(_table),
                  'select id_card, sentences, age from {} where age = 20 limit 5'.format(_table),
                  'select id_card, sentences, age, score  from {} where score = 60 limit 5'.format(_table),
                  'select id_card, sentences, age from {} where age > 40 limit 5'.format(_table),
                  'select id_card, sentences, age from {} where age < 10 limit 5'.format(_table),
                  'select id_card, sentences, age from {} where age <= 10 limit 5'.format(_table),
                  'select id_card, sentences, age, score from {} where score > 70 limit 5'.format(_table),
                  'select * from {} where id_card = "310310314" and sentences = "杭州光之树大华公司中国集团上海腾讯有限"'.format(_table),
                  'select * from {} where id_card = "310310320" or sentences = "杭州光之树大华中国公司集团上海腾讯有限"'.format(_table),
                  'select id_card, sentences, age from {} order by age limit 5'.format(_table),
                  'select distinct id_card, sentences, age from {} limit 5'.format(_table),
                  'select distinct age, sentences, id_card from {} limit 5'.format(_table),
                  'select max(age), min(age) from {}'.format(_table),
                  'select id_card, sentences from {} WHERE age = 30 limit 5'.format(_table),
                  'select max(score), min(score) from {} '.format(_table),
                  'select max(score), min(age) from {} '.format(_table),
                  'select sum(score) from {} '.format(_table),
                  'select sum(age) from {} '.format(_table),
                  'select sum(age), avg(age) from {} '.format(_table),
                  'select min(weight), max(weight) from {} '.format(_table),
                  'select min(height), max(height) from {} '.format(_table),
                  'select id_card, sentences from {} where sentences like "%杭州光之树%" limit 5'.format(_table),
                  'select id_card, sentences, comments from {} where sentences like "光之树%中国上%" limit 5'.format(_table),
                  ]
        for sql in select:
            r0, r1 = req_select(_db_info, _cx, sql)
            r0 = [tuple(i) for i in r0]
            # print('r0',r0)
            # print('r1',r1)
            try:
                self.assertEqual(r0, r1, 'The sql result is not equal {}'.format(sql))
            except Exception as e:
                print(e)
        print("finished {}".format(_table))
        print("-" * 100)

    def test_two_tables(self, _db_info, _cx, _table1, _table2):
        select = \
            [
                'SELECT {}.id_card, {}.age, {}.age FROM {} INNER JOIN {} ON {}.age = {}.age limit 10'
                 .format(_table1, _table1, _table2, _table1, _table2, _table1, _table2),
                # fixme:bug
                'SELECT {}.id_card, {}.age, {}.age FROM {} INNER JOIN {} ON {}.age = {}.score limit 10'
                    .format(_table1, _table1, _table2, _table1, _table2, _table1, _table2),
                f'SELECT {_table1}.id_card FROM {_table1} LEFT JOIN {_table2} ON {_table1}.id_card = {_table2}.id_card limit 10',
                'SELECT {}.id_card, {}.score, {}.score FROM {} LEFT JOIN {} ON {}.score = {}.score limit 10'
                    .format(_table1, _table1, _table2, _table1, _table2, _table1, _table2),
                'SELECT {}.id_card, {}.score, {}.score FROM {} RIGHT JOIN {} ON {}.score = {}.score limit 10'
                    .format(_table1, _table1, _table2, _table1, _table2, _table1, _table2),
                'SELECT {}.id_card, {}.age, {}.age FROM {} RIGHT JOIN {} ON {}.age = {}.age limit 10'
                    .format(_table1, _table1, _table2, _table1, _table2, _table1, _table2),
            ]
        for sql in select:
            r0, r1 = req_select(_db_info, _cx, sql)
            r0 = [tuple(i) for i in r0]
            try:
                self.assertEqual(r0, r1, 'The sql result is not equal {}'.format(sql))
            except Exception as e:
                print(e)
        print("finished {} {}".format(_table1, _table2))
        print("-" * 100)

    @staticmethod
    def update_sql(_db_info, _cx, _table):
        print("update table {}".format(_table))
        print("--" + "\n" + "--")
        update =[
            'UPDATE {} set id_card = "310310319" , sentences = "上海市光之树大华" WHERE age = 30'.format(_table)
        ]
        for sql in update:
            req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def delete_sql(_db_info, _cx, _table):
        print("delete table {}".format(_table))
        print("--" + "\n" + "--")
        delete = [
            'DELETE FROM {} WHERE id_card = "310310319" and sentences = "上海市光之树大华" '.format(_table)
        ]
        for sql in delete:
            req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-" * 100)


class DataBase:

    @staticmethod
    def get_db_info():
        db_host = '127.0.0.1'
        db = 'points'
        user = 'root'
        password = 'root'
        port = 3306

        db_info = {
            'host': db_host, 'db': db,
            'user': user, 'password': password,
            'port': port
        }
        return db_info

    @staticmethod
    def get_mysql_cu(db_info):
        _cx = mysql.connector.connect(
            host=db_info["host"],
            database=db_info["db"],
            user=db_info["user"],
            passwd=db_info["password"],
            port=int(db_info["port"])
        )
        return _cx


if __name__ == "__main__":
    database_info = DataBase.get_db_info()
    mysql_cx = DataBase.get_mysql_cu(database_info)

    e2e = E2ETest()

    # test1
    table = e2e.create_table(database_info, mysql_cx)
    e2e.add_primary_key(database_info, mysql_cx, table)
    e2e.drop_primary_key(database_info, mysql_cx, table)
    e2e.add_foreign_key(database_info, mysql_cx, table, refer_table='table_spa_test')
    e2e.drop_foreign_key(database_info, mysql_cx, table, refer_table='table_spa_test')
    e2e.drop_table(database_info, mysql_cx, table)

    # test2
    table = e2e.create_table(database_info, mysql_cx)
    e2e.insert_sql(database_info, mysql_cx, table, 50)
    e2e.test_select_sql(database_info, mysql_cx, table)
    e2e.update_sql(database_info, mysql_cx, table)
    e2e.delete_sql(database_info, mysql_cx, table)
    e2e.test_select_sql(database_info, mysql_cx, table)
    e2e.drop_table(database_info, mysql_cx, table)

    # test3
    table1 = e2e.create_table(database_info, mysql_cx)
    table2 = e2e.create_table(database_info, mysql_cx)
    e2e.insert_sql(database_info, mysql_cx, table1)
    e2e.insert_sql(database_info, mysql_cx, table2)
    e2e.test_two_tables(database_info, mysql_cx, table1, table2)
    e2e.drop_table(database_info, mysql_cx, table1)
    e2e.drop_table(database_info, mysql_cx, table2)
