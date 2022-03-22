import re
import json
import yaml
import sqlite3
from copy import deepcopy

from sql_parsing import parse_mysql as parse
from sql_parsing import format

import mysql.connector


with open("config.yaml", 'r', encoding='utf-8') as f:
    cfg = f.read()
    config = yaml.full_load(cfg)
    if config['meta']['type'] == 'mysql':
        cx = mysql.connector.connect(
              host=config['meta']['mysql']["host"],
              database=config['meta']['mysql']["db"],
              user=config['meta']['mysql']["user"],
              passwd=config['meta']['mysql']["passwd"],
              port=int(config['meta']['mysql']["port"])
            )
        insert_sql = 'insert into p_db_meta values(%s,%s,%s,%s,%s,%s)'
    else:
        cx = sqlite3.connect(config['meta']['sqlite'], check_same_thread=False)
        insert_sql = 'insert into p_db_meta values (?,?,?,?,?,?)'


def get_type_and_len(t_k, t_v):
    if t_k in ['char', 'varchar']:
        i = 0 if (t_v // 16 + 1) % 3 == 0 else 1
        n = (((t_v // 16 + 1) * 16) // 3 + i) * 4
        t_k = 'varchar' if n > 254 else t_k
        return t_k, n
    else:
        return t_k, t_v


element_type = {
    "INT":
        {
            "order-preserving": "BIGINT",
            "symmetric": "VARCHAR(300)"
        },
    "VARCHAR":
        {
            "symmetric": get_type_and_len
        },
    "CHAR":
        {
            "symmetric": get_type_and_len
        },
    "FLOAT":
        {
            "order-preserving": "DECIMAL(65)",
            "symmetric": "VARCHAR(1000)"
        },
    "DOUBLE":
        {
            "order-preserving": "DECIMAL(65)",
            "symmetric": "VARCHAR(1000)"
        }
}


fuzzy_type_fixed = 'VARCHAR(2000)'
arithmetic_type_fixed = 'VARCHAR(2000)'


FUNC_CIPHERS = {
    "max": "order-preserving",
    "min": "order-preserving",
    "sum": "arithmetic",
    "avg": "arithmetic"
}


class Delta(object):
    __instance = None
    meta = None
    table_json = None
    cx = None

    def __new__(cls, *args, **kwargs):
        if Delta.__instance is None:
            Delta.__instance = object.__new__(cls, *args, **kwargs)
            cls.cx = cx
            cls.meta = cls.load_delta()
        return Delta.__instance

    def update_delta(self, db_name, table_meta):
        if self.meta:
            if db_name not in self.meta.keys():
                self.meta.update({db_name: table_meta})
            else:
                self.meta[db_name].update(table_meta)

        else:
            self.meta = {
                db_name: table_meta
            }
        # update database meta
        self.update_db_meta(db_name, table_meta)
        return self.meta

    def delete_delta(self, db, table):
        self.meta[db].pop(table, None)
        cu = self.cx.cursor()
        sql = "delete from p_db_meta where database_name = '{}' and table_name = '{}'".format(db, table)
        cu.execute(sql)
        if config['meta']['type'] == 'mysql':
            self.cx.commit()
        else:
            cu.connection.commit()

    def update_db_meta(self, db_name, table_meta):
        cu = self.cx.cursor()
        rows = []
        for t_name, t_value in table_meta.items():
            table_name = t_name
            table_anonymous = t_value['anonymous']
            for key, value in t_value['columns'].items():
                col_str = json.dumps({key: value})
                rows.append((db_name, 'mysql', table_name, table_anonymous, col_str, ''))
        cu.executemany(insert_sql, rows)
        if config['meta']['type'] == 'mysql':
            self.cx.commit()
        else:
            cu.connection.commit()

    @classmethod
    def load_delta(cls):
        meta = {}
        cu = cls.cx.cursor()
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
        return meta

    @staticmethod
    def create_paillier_sum_procedure(cursor, feature, table_name, use_cursor):
        if not use_cursor:
            set_concat_length_global_query = 'SET GLOBAL group_concat_max_len = 18446744073709551615'
            set_concat_length_session_query = 'SET SESSION group_concat_max_len = 18446744073709551615'
            cursor.execute(set_concat_length_global_query)
            cursor.execute(set_concat_length_session_query)
            return
        feature_name, n_square = feature
        drop_procedure = "drop procedure if exists paillierSum"
        cursor.execute(drop_procedure)
        new_feature_name = feature_name.replace('.', '_')
        create_procedure = f"CREATE PROCEDURE `paillierSum`(IN nSquare DECIMAL(65,0), OUT sum{new_feature_name} DECIMAL(65,0), OUT num{new_feature_name} DECIMAL(65,0))" \
                         f"BEGIN DECLARE done BOOLEAN DEFAULT 0; DECLARE o DECIMAL(65,0); DECLARE enc_data CURSOR FOR SELECT {feature_name} from {table_name};DECLARE CONTINUE HANDLER FOR NOT FOUND SET done=1;set sum{new_feature_name}=1;set num{new_feature_name}=0;OPEN enc_data;fetch_loop: LOOP FETCH enc_data INTO o;IF done THEN LEAVE fetch_loop; END IF; set sum{new_feature_name} = (sum{new_feature_name}*o)%nSquare; set num{new_feature_name}= num{new_feature_name}+1; END LOOP; CLOSE enc_data;" \
                         "END"
        cursor.execute(create_procedure)
        set_query = "SET @nSquare = {};".format(n_square)
        call_query = "CALL `paillierSum`(@nSquare, @sum{}, @num{});".format(new_feature_name, new_feature_name)
        cursor.execute(set_query)
        cursor.execute(call_query)

    @classmethod
    def get_paillier_procedure_info(cls, enc_query, db, table_name):
        sum_feature_name_list = []
        avg_feature_name_list = []
        if cls.table_json:
            json = deepcopy(cls.table_json)
        else:
            json = parse(enc_query)
        json.pop('limit', None)
        json_select = json['select']
        if not isinstance(json_select, list):
            json_select = [json_select['value']]
        homo_enc_dict = {}
        columns_info = cls.meta[db][table_name]['columns']
        for col, col_info in columns_info.items():
            if col_info['enc-cols'].get('arithmetic'):
                n_square = (col_info['homomorphic_key'][0] * col_info['homomorphic_key'][1])**2
                homo_enc_dict.update({col_info['enc-cols']['arithmetic']: n_square})
        for value in json_select:
            if isinstance(value, dict):
                try:
                    if value.get('sum', '').split('.')[-1] in homo_enc_dict:
                        sum_feature_name_list.append((value['sum'], homo_enc_dict[value['sum'].split('.')[-1]]))
                    if value.get('avg', '').split('.')[-1] in homo_enc_dict:
                        avg_feature_name_list.append((value['avg'], homo_enc_dict[value['avg'].split('.')[-1]]))
                except:
                    pass
        need_paillier_procedure = len(sum_feature_name_list) + len(avg_feature_name_list)
        table_name = None
        if need_paillier_procedure:
            enc_query = format(json)
            table_name = enc_query[re.search('FROM ', enc_query).span()[1]:]
        return sum_feature_name_list, avg_feature_name_list, need_paillier_procedure, table_name, enc_query

    @staticmethod
    def modify_sum_query(enc_query, feature_name, use_cursor):
        new_feature_name = feature_name.replace('.', '_')
        if not use_cursor:
            return enc_query.replace('SUM({})'.format(feature_name), "CONCAT(GROUP_CONCAT({}), ',SUM')".format(new_feature_name))
        return enc_query.replace('SUM({})'.format(feature_name), '@sum{}'.format(new_feature_name))

    @staticmethod
    def modify_avg_query(enc_query, feature_name, use_cursor):
        new_feature_name = feature_name.replace('.', '_')
        if not use_cursor:
            return enc_query.replace('AVG({})'.format(feature_name), "CONCAT(GROUP_CONCAT({}), ',AVG')".format(new_feature_name))
        return enc_query.replace('AVG({})'.format(feature_name), "concat(@sum{}, ',',@num{})".format(new_feature_name,
                                                                                                     new_feature_name))
