import logging
import numpy as np
import base64
import time
import json
import requests
import mysql.connector
import datetime
import os
import mysql.connector

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('../euler_db.log')
handler.setLevel(logging.INFO)
logger.addHandler(handler)


class DataBase:

    @staticmethod
    def get_db_info():
        db_host = '192.168.51.170'
        db = 'dahua_yuanqu_test'
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


def generate_face_id(face):
    b = face.tobytes()
    # c = np.frombuffer(b, dtype=np.float64)
    d = base64.b64encode(b).decode("utf-8")
    return d


def generate_file(num):
    _file_name = os.path.join(os.getcwd(), "data.csv")
    a = np.random.random((200000, 64))
    import csv
    with open(_file_name, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for i in range(num):
            line = [generate_face_id(a[i%200000]), str(i%400), str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]
            writer.writerow(line)
    return _file_name


def remove_file(_file_name):
    os.remove(_file_name)


class E2E():
    @staticmethod
    def drop_table(_db_info, _cx, _table):
        sql = 'drop table {}'.format(_table)
        print("drop table {}".format(_table))
        print("--" + "\n" + "--")
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
    e2e.drop_table(database_info, mysql_cx, "shuaka_" + str(1000000))
    e2e.drop_table(database_info, mysql_cx, "shuaka_" + str(2000000))
    e2e.drop_table(database_info, mysql_cx, "shuaka_" + str(4000000))
    e2e.drop_table(database_info, mysql_cx, "shuaka_" + str(8000000))
    e2e.drop_table(database_info, mysql_cx, "shuaka_" + str(12000000))
    e2e.drop_table(database_info, mysql_cx, "shuaka_" + str(15000000))
