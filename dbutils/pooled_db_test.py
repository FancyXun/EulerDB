import unittest
import mysql.connector as mysql_conn
from dbutils.pooled_db import PooledDB


class MyTestCase(unittest.TestCase):
    def test_something(self):
        pool = PooledDB(mysql_conn, 5, host='localhost', user='root', passwd='root', db='points', port=3306)
        conn = pool.connection()
        cur = conn.cursor()
        sql = "select * from TABEL_77f682fd57b5fb627bd1f90996db016b limit 10"
        cur.execute(sql)
        r = cur.fetchall()
        print(r)


if __name__ == '__main__':
    unittest.main()
