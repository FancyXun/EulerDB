import logging
import hashlib
import time
import json
import requests
import mysql.connector
import random
import os
import mysql.connector
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('../euler_db.log')
handler.setLevel(logging.INFO)
logger.addHandler(handler)


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


def req(_db_info, _cx, sql):
    _db_info['query'] = sql
    requests.post('http://localhost:8888/query', json.dumps(_db_info))
    start_time = time.time()
    cu = _cx.cursor()
    cu.execute(sql)
    _cx.commit()
    cu.close()
    logger.info("mysql:{}".format(time.time() - start_time))


def AllRange(sentences, words, start, end):
    if start == end:
        sentences.append("".join(words))
    for m in range(start, end + 1):
        words[m], words[start] = words[start], words[m]
        AllRange(sentences, words, start + 1, end)
        words[m], words[start] = words[start], words[m]


def generate_words():
    sentences = []
    words = ['杭州', '光之树', '大华', '中国', '集团', '公司', '上海', '腾讯', '有限',
             '因为', '研究生', '西交大', '世界', '著名的', '天才', '喜剧电影家']
    words = [words[random.randint(0, len(words) - 1)] for _ in range(8)]
    AllRange(sentences, words, 0, 5)
    words = ['张江', '阿里', '百度', '科技', '北京', '深圳', '广州', '字节跳动',
             '云计算方向', '研究院', '天气', '大合唱', '爱护身体', '活出自我', '笑对人生']
    words = [words[random.randint(0, len(words) - 1)] for _ in range(8)]
    AllRange(sentences, words, 0, 5)
    words = ['浙江', '疫情', '苏州', '南京', '成都', '四川', '重庆', '陕西', '灯光',
             '解决人口问题', '进行婚育补贴帮助', '人口生产任务', '高消费', '知道', '任何发生'
             '身边的事情']
    words = [words[random.randint(0, len(words) - 1)] for _ in range(8)]
    AllRange(sentences, words, 0, 5)
    return sentences


def generate_file(num):
    _file_name = os.path.join(os.getcwd(), "data.csv")
    sentences = generate_words()
    comments = generate_words()
    import csv
    with open(_file_name, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for i in range(num):
            if i == 1:
                line = [str(310310310 + i), "浙江大华技术股份有限公司", str(20), str(random.randint(60, 100)),
                        "浙江"]
            else:
                if not sentences:
                    sentences = generate_words()
                if not comments:
                    comments = generate_words()
                line = [str(310310310 + i), sentences.pop(), str(random.randint(1, 100)), str(random.randint(60, 100)),
                        comments.pop()]
            writer.writerow(line)
    return _file_name


def remove_file(_file_name):
    os.remove(_file_name)


class E2E():

    @staticmethod
    def test_create_table(_db_info, _cx):
        _table = "table_" + hashlib.md5(str(time.perf_counter()).encode('utf-8')).hexdigest()
        sql = 'create table if not exists {}(' \
              'id_card varchar(100), ' \
              'sentences varchar(300), ' \
              'age int, ' \
              'score int, ' \
              'comments varchar(100)) '.format(_table)

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

    @staticmethod
    def batch_insert_sql(_db_info, _cx, _table, _file_name, _columns):
        print("batch insert table {}".format(_table))
        print("--" + "\n" + "--")
        _db_info['batch_process'] = {
            'mode': 'insert',
            'table': _table,
            'columns': _columns,
            'input': _file_name
        }
        requests.post('http://localhost:8888/query', json.dumps(_db_info))
        _db_info.pop('batch_process')
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def batch_insert_mysql(_db_info, _cx, _table, _file_name, _columns):
        print("batch insert mysql table {}".format(_table))
        print("--" + "\n" + "--")
        chunk_size = 10 ** 4 * 2
        cu = _cx.cursor()
        for chunk in pd.read_csv(_file_name, chunksize=chunk_size, header=None):
            data = chunk.values.tolist()
            query = "insert into {} (".format(_table) + ",".join(_columns) + ") values(" \
                    + ",".join(["%s"] * len(_columns)) + ")"
            for row in data:
                cu.execute(query, tuple(row))
            _cx.commit()
        cu.close()
        print("finished".format(_table))
        print("-" * 100)

    @staticmethod
    def select_sql(_db_info, _cx, _table):
        print("select table {}".format(_table))
        print("--" + "\n" + "--")
        select = [
            'select * from {} where sentences = "浙江大华技术股份有限公司" '.format(_table),
            'select id_card, sentences, age from {} order by age limit 1'.format(_table),
            'select max(score), min(age) from {}'.format(_table),
            'select id_card, sentences from {} where sentences like "浙江大华技术股份%"'.format(_table),
        ]
        for sql in select:
            r0, r1 = req_select(_db_info, _cx, sql)
            r0 = [tuple(i) for i in r0]
            try:
                assert r0 == r1, "not equal"
            except Exception as e:
                print(e)
        print("finished {}".format(_table))
        print("-" * 100)

    @staticmethod
    def update_sql(_db_info, _cx, _table):
        print("update table {}".format(_table))
        print("--" + "\n" + "--")
        update = [
            'UPDATE {} set id_card = "310310319" , sentences = "上海市光之树大华" WHERE age = 30'.format(_table)
        ]
        for sql in update:
            req(_db_info, _cx, sql)
        print("finished".format(_table))
        print("-" * 100)


def req_select(_db_info, _cx, sql):
    _db_info['query'] = sql
    result = requests.post('http://localhost:8888/query', json.dumps(_db_info))
    start_time = time.time()
    cu = _cx.cursor()
    cu.execute(sql)
    mysql_result = cu.fetchall()
    logger.info("mysql:{}".format(time.time() - start_time))
    return result.json()['result'], mysql_result


if __name__ == '__main__':
    database_info = DataBase.get_db_info()
    mysql_cx = DataBase.get_mysql_cu(database_info)
    e2e = E2E()
    table1 = e2e.test_create_table(database_info, mysql_cx)
    for i in range(100):
        file_name = generate_file(100000)
        columns = ['id_card', 'sentences', 'age', 'score', 'comments']
        start = time.time()
        e2e.batch_insert_sql(database_info, mysql_cx, table1, file_name, columns)
        e2e.batch_insert_mysql(database_info, mysql_cx, table1, file_name, columns)
        e2e.select_sql(database_info, mysql_cx, table1)
        e2e.update_sql(database_info, mysql_cx, table1)
        remove_file(file_name)
