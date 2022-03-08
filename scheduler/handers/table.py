import time
import hashlib
import string
import random

from sql_parsing import parse_mysql as parse
from scheduler.schema.metadata import Delta
from scheduler.schema.metadata import element_type, fuzzy_type_fixed


def rewrite_table(db, db_meta, query, encrypted_cols):
    json = parse(query)
    if 'create table' not in json.keys():
        raise Exception("Bad create table query: {}".format(query))
    create_table = json['create table']
    table_name = create_table['name']
    if table_name in db_meta.keys():
        raise Exception("Table {} already exists".format(table_name))
    columns = create_table['columns']
    enc_columns = []
    columns_kv = {}
    columns = columns if isinstance(columns, list) else [columns]
    for col in columns:
        col_name = col['name']
        col_type = col['type']
        columns_kv[col_name] = {}
        columns_kv[col_name]['enc-cols'] = {}
        if col['name'] not in encrypted_cols:
            for t_k, t_v in col_type.items():
                if t_v:
                    enc_col_type = t_k + '(' + str(t_v) + ')'
                else:
                    enc_col_type = t_k
                enc_columns.append(col_name + ' ' + enc_col_type)
                columns_kv[col_name]['plaintext'] = True
                columns_kv[col_name]['type'] = t_k
                columns_kv[col_name]['key'] = ""
            continue
        val = element_type.get(list(col['type'].keys())[0].upper())
        for k, v in val.items():
            enc_col_name = random.choice(string.ascii_letters) + \
                           hashlib.md5(str(time.clock()).encode('utf-8')).hexdigest()
            for t_k, t_v in col_type.items():
                columns_kv[col_name]['type'] = t_k
                if t_v:
                    enc_col_type = t_k + '(' + str(t_v * v) + ')'
                else:
                    enc_col_type = v
                enc_columns.append(enc_col_name + ' ' + enc_col_type)
                columns_kv[col_name]['enc-cols'].update({k: enc_col_name})
        if encrypted_cols[col['name']]['fuzzy']:
            enc_col_name = random.choice(string.ascii_letters) + \
                           hashlib.md5(str(time.clock()).encode('utf-8')).hexdigest()
            enc_columns.append(enc_col_name + ' ' + fuzzy_type_fixed)
            columns_kv[col_name]['enc-cols'].update({"fuzzy": enc_col_name})
        columns_kv[col_name]['key'] = encrypted_cols[col['name']]['key']
        columns_kv[col_name]['plaintext'] = False
    anonymous_table = "table_" + hashlib.md5(str(time.clock()).encode('utf-8')).hexdigest()
    query = 'CREATE TABLE ' + anonymous_table + '(' + ",".join(enc_columns) + ');'
    table_meta = {
        table_name: {
            'anonymous': anonymous_table,
            'columns': columns_kv
        }
    }
    Delta().update_delta(db, table_meta)
    return query, {'table': table_name}
