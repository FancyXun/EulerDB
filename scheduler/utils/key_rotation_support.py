import json
import pandas as pd
import mysql.connector
from scheduler.utils import decrypt_support
from scheduler.crypto import encrypt


class KeyRotation:

    @classmethod
    def rotate_key(cls, db_info, proxy_db_info, table, feature, new_aes_key, csv_path=''):
        cipher = cls.get_raw_data(csv_path=csv_path)
        aes_key, enc_feature_name, enc_table = cls.get_key_info(proxy_db_info, db_info['db'], table, feature)
        cls.drop_data(db_info, enc_table, enc_feature_name)
        raw_data = cls.decrypt_data(cipher, aes_key=aes_key)
        encrypted_data = cls.encrypt_data(raw_data, new_aes_key)
        cls.upload_encrypted_data(db_info, enc_table, enc_feature_name, encrypted_data)
        cls.update_proxy_db(proxy_db_info, db_info['db'], table, feature, new_aes_key)

    @staticmethod
    def drop_data(db_info, table, feature):
        db_conn = mysql.connector.connect(
              host=db_info["host"],
              database=db_info["db"],
              user=db_info["user"],
              passwd=db_info["passwd"],
              port=int(db_info["port"])
            )
        cu = db_conn.cursor()
        sql = f'UPDATE {table} SET {feature} = '';'
        cu.execute(sql)
        return

    @staticmethod
    def get_raw_data(**kwargs):
        if kwargs.get('csv_path'):
            return pd.read_csv(kwargs.get('csv_path'), header=0).values

    @staticmethod
    def get_key_info(proxy_db_info, db, table, feature):
        db_conn = mysql.connector.connect(
              host=proxy_db_info["host"],
              database=proxy_db_info["db"],
              user=proxy_db_info["user"],
              passwd=proxy_db_info["passwd"],
              port=int(proxy_db_info["port"])
            )
        meta = {}
        cu = db_conn.cursor()
        cu.execute("select * from p_db_meta")
        db_meta = cu.fetchall()
        if not db_meta:
            return meta
        for row in db_meta:
            db_name, table_name, table_anonymous, col_info = row[0], row[2], row[3], row[4]
            if db_name not in meta.keys():
                meta[db_name] = {}
            if table_name not in meta[db_name].keys():
                meta[db_name][table_name] = {}
            if 'anonymous' not in meta[db_name][table_name].keys():
                meta[db_name][table_name]['anonymous'] = table_anonymous
            if 'columns' not in meta[db_name][table_name].keys():
                meta[db_name][table_name]['columns'] = {}
            meta[db_name][table_name]['columns'].update(json.loads(col_info))
        db_meta = meta[db]
        key = db_meta[table]['columns'][feature]['key']
        return encrypt.AESCipher(key), db_meta[table]['columns'][feature], db_meta[table]['anonymous']

    # todo some bug need to fix
    @staticmethod
    def update_proxy_db(proxy_db_info, db, table, feature, new_aes_key):
        db_conn = mysql.connector.connect(
              host=proxy_db_info["host"],
              database=proxy_db_info["db"],
              user=proxy_db_info["user"],
              passwd=proxy_db_info["passwd"],
              port=int(proxy_db_info["port"])
            )
        meta = {}
        cu = db_conn.cursor()
        cu.execute("select * from p_db_meta")
        db_meta = cu.fetchall()
        if not db_meta:
            return meta
        for row in db_meta:
            db_name, table_name, table_anonymous, col_info = row[0], row[2], row[3], row[4]
            if db_name not in meta.keys():
                meta[db_name] = {}
            if table_name not in meta[db_name].keys():
                meta[db_name][table_name] = {}
            if 'anonymous' not in meta[db_name][table_name].keys():
                meta[db_name][table_name]['anonymous'] = table_anonymous
            if 'columns' not in meta[db_name][table_name].keys():
                meta[db_name][table_name]['columns'] = {}
            meta[db_name][table_name]['columns'].update(json.loads(col_info))
        meta[db][table]['columns'][feature]['key'] = new_aes_key

        rows = []
        for t_name, t_value in meta[db][table].items():
            table_name = t_name
            table_anonymous = t_value['anonymous']
            for key, value in t_value['columns'].items():
                col_str = json.dumps({key: value})
                rows.append((db, 'mysql', table_name, table_anonymous, col_str, ''))
        cu.executemany('insert into p_db_meta values(%s,%s,%s,%s,%s,%s)', rows)
        db_conn.commit()


    @staticmethod
    def upload_encrypted_data(db_info, enc_table_name, enc_feature_name, cipher_list):
        db_conn = mysql.connector.connect(
              host=db_info["host"],
              database=db_info["db"],
              user=db_info["user"],
              passwd=db_info["passwd"],
              port=int(db_info["port"])
            )
        cu = db_conn.cursor()
        for cipher in cipher_list:
            sql = f'insert into {enc_table_name}({enc_feature_name}) values({cipher})'
            cu.execute(sql)

    @staticmethod
    def encrypt_data(raw_data, new_key):
        res = []
        for i in raw_data:
            res.append(new_key.encrypt(i))
        return res

    @staticmethod
    def decrypt_data(cipher_list, aes_key):
        raw_data = decrypt_support.OfflineDecryption.pool_decrypt(cipher_list, aes_key, 'str')
        return raw_data


if __name__ == '__main__':
    db_info = {"host": 'localhost', 'db': 'test', 'user': 'root', 'passwd': 'root', 'port': 3306}
    proxy_db_info = db_info
    KeyRotation.rotate_key(db_info, proxy_db_info, 'test_table', 'test_feature',
                           "abcdefghijklmnopqrstuvwxyz@#$%^&*()points", 'test.csv')


