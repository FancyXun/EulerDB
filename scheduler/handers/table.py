import time
import hashlib

from sql_parsing import parse_mysql as parse
from scheduler.schema.metadata import Delta
from scheduler.schema.metadata import \
    ENCRYPT_SQL_TYPE, \
    FUZZY_TYPE


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
        columns_kv[col_name]['ENC_COLUMNS'] = {}
        if col['name'] not in encrypted_cols:
            for t_k, t_v in col_type.items():
                if t_v:
                    enc_col_type = t_k + '(' + str(t_v) + ')'
                else:
                    enc_col_type = t_k
                enc_columns.append(col_name + ' ' + enc_col_type)
                columns_kv[col_name]['PLAINTEXT'] = True
                columns_kv[col_name]['TYPE'] = t_k
            continue
        val = ENCRYPT_SQL_TYPE.get(list(col['type'].keys())[0].upper())
        for k, v in val.items():
            enc_col_name = col_name.upper() + k
            for t_k, t_v in col_type.items():
                columns_kv[col_name]['TYPE'] = t_k
                if t_v:
                    enc_col_type = t_k + '(' + str(t_v * v) + ')'
                else:
                    enc_col_type = v
                enc_columns.append(enc_col_name + ' ' + enc_col_type)
                columns_kv[col_name]['ENC_COLUMNS'].update({k: enc_col_name})
        if encrypted_cols[col['name']]['fuzzy']:
            enc_col_name = col_name.upper() + 'FUZZY'
            enc_columns.append(enc_col_name + ' ' + FUZZY_TYPE)
            columns_kv[col_name]['ENC_COLUMNS'].update({"FUZZY": enc_col_name})
        columns_kv[col_name]['PLAINTEXT'] = False
    anonymous_table = "TABEL_" + hashlib.md5(str(time.clock()).encode('utf-8')).hexdigest()
    query = 'CREATE TABLE ' + anonymous_table + '(' + ",".join(enc_columns) + ');'
    table_meta = {
        table_name: {
            'anonymous': anonymous_table,
            'columns': columns_kv
        }
    }
    Delta().update_delta(db, table_meta)
    return query, {'table': table_name}
