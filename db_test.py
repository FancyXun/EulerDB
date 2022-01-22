import unittest
import random

import mysql.connector
from mysql.connector import Error

from handers.insert_hander import InsertHandler
from handers.create_hander import CreateTableHandler
from handers.select_handler import SelectHandler


def create_db(database):
    try:
        connection = mysql.connector.connect(host='127.0.0.1',
                                             user='root',
                                             password='root')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()

            sql = "create database if not exists " + database + ";"
            cursor.execute(sql)
            record = cursor.fetchone()
            print("You're connected to database: ", record)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def create_table(database, query):
    create_table_handler = CreateTableHandler(query)
    print(create_table_handler.query)
    try:
        connection = mysql.connector.connect(host='127.0.0.1',
                                             database=database,
                                             user='root',
                                             password='root')
        if connection.is_connected():
            db_Info = connection.get_server_info()
            print("Connected to MySQL Server version ", db_Info)
            cursor = connection.cursor()
            cursor.execute(create_table_handler.query)
    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def insert_data(database, query):
    insert_handler = InsertHandler(query)
    try:
        connection = mysql.connector.connect(host='127.0.0.1',
                                             database=database,
                                             user='root',
                                             password='root')
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(insert_handler.query)
            connection.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


def select_data(database, query):
    select_handler = SelectHandler(query)
    try:
        connection = mysql.connector.connect(host='127.0.0.1',
                                             database=database,
                                             user='root',
                                             password='root')
        if connection.is_connected():
            cursor = connection.cursor()
            cursor.execute(select_handler.query)
            connection.commit()

    except Error as e:
        print("Error while connecting to MySQL", e)
    finally:
        if connection.is_connected():
            cursor.close()
            connection.close()


class MyTestCase(unittest.TestCase):
    def test_db(self):
        # create database
        create_db('points')
        self.assertEqual(True, True)  # add assertion here

    def test_table(self):
        # create table
        create_table_sql = 'create table if not exists user(' \
                           'id int, name varchar(20), age int, sex varchar(5), ' \
                           'score int, nick_name varchar(20), comment varchar(200));'
        create_table('points', create_table_sql)
        self.assertEqual(True, True)  # add assertion here

    def test_insert(self):
        # insert data
        for i in range(10000):
            sql = 'insert into user(id, name, age, sex, score, nick_name, comment) values (' + str(
                random.randint(100000, 1000000)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 5)) + '",' + str(random.randint(1, 50)) + ', "' + ''.join(
                random.sample('fm', 1)) + '",' + str(random.randint(60, 100)) + ',"' + ''.join(
                random.sample('zyxwvutsrqponmlkjihgfedcba', 10)) + '","' + ''.join(
                random.sample('zyxwvutsrqpondgsghgsfgyftuywiecvdbhsbdhgshfdgsgfysgmlkjihgfedcba', 30)) + '")'
            insert_data('points', sql)
            print(sql)
        self.assertEqual(True, True)  # add assertion here

    def test_select_all(self):
        # create database
        sql = 'select * from user'
        select_data('points', sql)
        self.assertEqual(True, True)  # add assertion here


if __name__ == '__main__':
    unittest.main()
