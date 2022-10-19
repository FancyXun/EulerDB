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
import pandas as pd

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

    # 'id', 'device_id', 'time'
    @staticmethod
    def test_create_daka_table(_db_info, _cx, table_name):
        sql = 'create table if not exists {}(' \
              'id int, ' \
              'device_id int, ' \
              'time datetime) '.format(table_name)

        encrypted_columns = {
        }
        _db_info['encrypted_columns'] = encrypted_columns
        print("create table {}".format(table_name))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql)
        print("finished".format(table_name))
        print("-" * 100)
        _db_info.pop('encrypted_columns')
        return table_name

    @staticmethod
    def test_create_quanxian_table(_db_info, _cx, table_name):
        sql = 'create table if not exists {}(' \
              'id int, ' \
              'device_id int, ' \
              'quanxian int) '.format(table_name)

        encrypted_columns = {
        }
        _db_info['encrypted_columns'] = encrypted_columns
        print("create table {}".format(table_name))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql)
        print("finished".format(table_name))
        print("-" * 100)
        _db_info.pop('encrypted_columns')
        return table_name

    @staticmethod
    def test_create_user_info_table(_db_info, _cx, table_name):
        sql = 'create table if not exists {}(' \
              'face_id varchar(1000), ' \
              'name varchar(1000), ' \
              'addr varchar(1000), ' \
              'id_number varchar(1000), ' \
              'phone_number varchar(1000), ' \
              'age int, ' \
              'id int not null auto_increment primary key' \
              ') '.format(table_name)

        encrypted_columns = {
            "name": {
                "fuzzy": True,
                "key": "abcdefgpoints"
            },
            "addr": {
                "fuzzy": True,
                "key": "abcdefgpoints"
            },
            "id_number": {
                "fuzzy": True,
                "key": "abcdefgpoints"
            },
            "phone_number": {
                "fuzzy": True,
                "key": "abcdefgpoints"
            },
            "age": {
                "fuzzy": False,
                "key": "abcdefgpoints"
            },
        }
        _db_info['encrypted_columns'] = encrypted_columns
        print("create table {}".format(table_name))
        print("--" + "\n" + "--")
        req(_db_info, _cx, sql)
        print("finished".format(table_name))
        print("-" * 100)
        _db_info.pop('encrypted_columns')
        return table_name

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
            print(query)
            for row in data:
                print(row)
                cu.execute(query, tuple(row))
            _cx.commit()
        cu.close()
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


def gen_daka():
    import csv
    import datetime

    file_name = os.path.join(os.getcwd(), "daka.csv")
    with open(file_name, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for i in range(1000000):
            line = [i%200000, i%400, str(datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"))]
            writer.writerow(line)
    return file_name


def gen_user_info():
    import random

    def generate_face_id(face):
        return base64.b64encode(face.tobytes()).decode("utf-8")

    _file_name = os.path.join(os.getcwd(), "user_info.csv")
    a = np.random.random((200000, 64))
    names = ["张三", "李四", "小明"]
    addrs = ["杭州市滨江区滨兴路1399号", "上海市浦东新区张江科学城",
             "上海市浦东新区张江高科技园区碧波路690号", "北京海淀区上地十街10号百度大厦"]
    id_numbers = "310123198107"
    phone_number = "1332311"
    ages = np.random.randint(20, 100, 100)
    import csv
    with open(_file_name, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for i in range(200000):
            line = [generate_face_id(a[i % 200000]), names[i%3]+"_"+str(i), addrs[i%3]+"_"+str(i),
                    id_numbers+str(random.random()*1000000).split(".")[0],
                    phone_number + str(random.random()*1000000).split(".")[0],
                    ages[i % 100]]
            writer.writerow(line)
    return _file_name


def gen_quanxian():
    import csv
    file_name = os.path.join(os.getcwd(), "quanxian.csv")
    with open(file_name, "w") as csv_file:
        writer = csv.writer(csv_file, delimiter=',')
        for i in range(200000):
            a = np.random.randint(0, 2, 400)
            for j in range(400):
                line = [i, j, a[j]]
                writer.writerow(line)
    return file_name


if __name__ == '__main__':
    database_info = DataBase.get_db_info()
    mysql_cx = DataBase.get_mysql_cu(database_info)
    e2e = E2E()

    #插入打卡信息
    # columns = ['id', 'device_id', 'time']
    # table = "shuaka_history_1"
    # file_name =gen_daka()
    # e2e.test_create_daka_table(database_info, mysql_cx, table)
    # print("----------插入打卡信息--------")
    # e2e.batch_insert_sql(database_info, mysql_cx, table, file_name, columns)
    # e2e.batch_insert_mysql(database_info, mysql_cx, table, file_name, columns)
    # remove_file(file_name)

    # 插入个人信息
    # table = "user_info"
    # columns = ['face_id', 'name', 'addr', 'id_number', 'phone_number', 'age']
    # file_name = gen_user_info()
    # e2e.test_create_user_info_table(database_info, mysql_cx, "user_info")
    # print("----------插入个人信息--------")
    # e2e.batch_insert_sql(database_info, mysql_cx, table, file_name, columns)
    # e2e.batch_insert_mysql(database_info, mysql_cx, table, file_name, columns)
    # remove_file(file_name)

    # 插入权限
    columns = ['id', 'device_id', 'quanxian']
    table = "quanxian"
    file_name = gen_quanxian()
    e2e.test_create_quanxian_table(database_info, mysql_cx, table)
    print("----------插入权限--------")
    # e2e.batch_insert_sql(database_info, mysql_cx, table, file_name, columns)
    # e2e.batch_insert_mysql(database_info, mysql_cx, table, file_name, columns)
    # remove_file(file_name)

    # 复制数据
    print("----------复制数据--------")
    e2e.test_create_daka_table(database_info, mysql_cx, "shuaka_history_2")
    e2e.test_create_daka_table(database_info, mysql_cx, "shuaka_history_3")
    e2e.test_create_daka_table(database_info, mysql_cx, "shuaka_history_4")
    e2e.test_create_daka_table(database_info, mysql_cx, "shuaka_history_5")
    e2e.test_create_daka_table(database_info, mysql_cx, "shuaka_history_6")


'''
sql
insert into table_aff41894ff1ad23bc2587d5f9eaa9b33 select * from  table_b34acd68a1f4dd6c79a8dc9d8b50f749;
insert into table_aff41894ff1ad23bc2587d5f9eaa9b33 select * from  table_aff41894ff1ad23bc2587d5f9eaa9b33;
insert into table_2a4baa9b076ae36d3a288df98f27c47a select * from  table_aff41894ff1ad23bc2587d5f9eaa9b33;
insert into table_2a4baa9b076ae36d3a288df98f27c47a select * from  table_2a4baa9b076ae36d3a288df98f27c47a;
insert into table_90131ca79fc2a13ae5e35404268f06a3 select * from  table_2a4baa9b076ae36d3a288df98f27c47a;
insert into table_90131ca79fc2a13ae5e35404268f06a3 select * from  table_90131ca79fc2a13ae5e35404268f06a3;
insert into table_f2ab36cadb5cdbb7500b9b3008bef404 select * from  table_90131ca79fc2a13ae5e35404268f06a3;
insert into table_f2ab36cadb5cdbb7500b9b3008bef404 select * from  table_2a4baa9b076ae36d3a288df98f27c47a;
insert into table_3100d0ca755182d9e8821233ee59fe3a select * from  table_f2ab36cadb5cdbb7500b9b3008bef404;
insert into table_3100d0ca755182d9e8821233ee59fe3a select * from  table_2a4baa9b076ae36d3a288df98f27c47a limit 3000000;

'''