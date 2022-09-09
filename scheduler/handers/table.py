import time
import hashlib
import string
import random

from sql_parsing import parse_mysql as parse
from scheduler.crypto import encrypt
from scheduler.schema.metadata import Delta
from scheduler.schema.metadata import element_type, fuzzy_type_fixed, arithmetic_type_fixed, sm4_type_fixed


def rewrite_table(db, db_meta, query, encrypted_cols):
    json = parse(query)
    if 'create table' not in json.keys():
        raise Exception("Bad create table query: {}".format(query))
    create_table = json['create table']
    table_name = create_table['name']
    # if table_name in db_meta.keys():
    #     raise Exception("Table {} already exists".format(table_name))
    columns = create_table['columns']
    enc_columns = []
    columns_kv = {}
    columns = columns if isinstance(columns, list) else [columns]
    for col in columns:
        auto_increment = '' if not col.get('auto_increment') else ' auto_increment '
        primary_key = '' if not col.get('primary_key') else ' primary key' + auto_increment
        col_name = col['name']
        col_type = col['type']
        columns_kv[col_name] = {}
        columns_kv[col_name]['enc-cols'] = {}
        if 'nullable' in col:
            is_nullable = ' NOT NULL ' if not col.get('nullable') else ' NULL '
        else:
            is_nullable = ''
        if col.get('comment'):
            columns_kv[col_name]['comment'] = col.get('comment', {}).get('literal')
        if col['name'] not in encrypted_cols:
            for t_k, t_v in col_type.items():
                if t_v:
                    enc_col_type = t_k + '(' + str(t_v) + ')'
                else:
                    enc_col_type = t_k
                if col.get('default'):
                    default_value = col['default']
                    if isinstance(default_value, dict):
                        if 'null' in default_value:
                            default_value = 'NULL '
                    default_value = ' DEFAULT ' + default_value
                else:
                    default_value = ''
                enc_columns.append(col_name + ' ' + enc_col_type + is_nullable + primary_key + default_value)
                columns_kv[col_name]['plaintext'] = True
                columns_kv[col_name]['type'] = t_k
                columns_kv[col_name]['key'] = ""
            continue
        val = element_type.get(list(col['type'].keys())[0].upper())
        for k, v in val.items():
            primary_key_plus = primary_key if k == 'symmetric' else ''
            enc_col_name = random.choice(string.ascii_letters) + \
                           hashlib.md5(str(time.perf_counter()).encode('utf-8')).hexdigest()
            for t_k, t_v in col_type.items():
                columns_kv[col_name]['type'] = t_k
                if t_v:
                    t_k, t_l = v(t_k, t_v)
                    enc_col_type = t_k + '(' + str(t_l) + ')'
                else:
                    enc_col_type = v
                if col.get('default'):
                    default_value = col['default']
                    if isinstance(default_value, dict):
                        if 'null' in default_value:
                            default_value = 'NULL '
                    key = encrypted_cols[col['name']]['key']
                    default_value = ' DEFAULT ' + encrypt.AESCipher(key).encrypt(default_value)
                else:
                    default_value = ''
                enc_columns.append(enc_col_name + ' ' + enc_col_type + is_nullable + primary_key_plus + default_value)
                columns_kv[col_name]['enc-cols'].update({k: enc_col_name})
        if encrypted_cols[col['name']]['fuzzy']:
            enc_col_name = random.choice(string.ascii_letters) + \
                           hashlib.md5(str(time.perf_counter()).encode('utf-8')).hexdigest()
            if col.get('default'):
                default_value = col['default']
                if isinstance(default_value, dict):
                    if 'null' in default_value:
                        default_value = 'NULL '
                key = encrypted_cols[col['name']].get('sm4', "")
                default_value = ' DEFAULT ' + encrypt.SM4CipherBytes(key).encrypt(default_value)
            else:
                default_value = ''
            enc_columns.append(enc_col_name + ' ' + fuzzy_type_fixed + is_nullable + default_value)
            columns_kv[col_name]['enc-cols'].update({"fuzzy": enc_col_name})
        if encrypted_cols[col['name']].get('arithmetic'):
            enc_col_name = random.choice(string.ascii_letters) + \
                           hashlib.md5(str(time.perf_counter()).encode('utf-8')).hexdigest()
            if col.get('default'):
                default_value = col['default']
                if isinstance(default_value, dict):
                    if 'null' in default_value:
                        default_value = 'NULL '
                key = encrypted_cols[col['name']].get('homomorphic_key', "")
                default_value = ' DEFAULT ' + encrypt.HomomorphicCipher(key).encrypt(default_value)
            else:
                default_value = ''
            enc_columns.append(enc_col_name + ' ' + arithmetic_type_fixed + is_nullable + default_value)
            columns_kv[col_name]['enc-cols'].update({"arithmetic": enc_col_name})
            columns_kv[col_name]['homomorphic_key'] = encrypted_cols[col['name']].get('homomorphic_key', "")
        if encrypted_cols[col['name']].get('sm4'):
            enc_col_name = random.choice(string.ascii_letters) + \
                           hashlib.md5(str(time.perf_counter()).encode('utf-8')).hexdigest()
            if col.get('default'):
                default_value = col['default']
                if isinstance(default_value, dict):
                    if 'null' in default_value:
                        default_value = 'NULL '
                key = encrypted_cols[col['name']].get('sm4_key', "")
                default_value = ' DEFAULT ' + encrypt.SM4CipherBytes(key).encrypt(default_value)
            else:
                default_value = ''
            enc_columns.append(enc_col_name + ' ' + sm4_type_fixed + is_nullable + default_value)
            columns_kv[col_name]['enc-cols'].update({"sm4": enc_col_name})
            columns_kv[col_name]['sm4_key'] = encrypted_cols[col['name']].get('sm4_key', "")

        columns_kv[col_name]['key'] = encrypted_cols[col['name']]['key']
        columns_kv[col_name]['plaintext'] = False
    anonymous_table = "table_" + hashlib.md5(str(time.perf_counter()).encode('utf-8')).hexdigest()
    enc_constraint = []
    constraints = create_table.get('constraint', [])
    constraints = [constraints] if isinstance(constraints, dict) else constraints
    for constraint in constraints:
        # print('constraint', constraint)
        enc_constraint.append(get_constraint_key(constraint, columns_kv, db))
    enc_constraint = "," + ",".join(enc_constraint) if enc_constraint else ""
    engine = 'ENGINE = ' + json.get('engine') if json.get('engine') else ''
    table_auto_increment = 'AUTO_INCREMENT = ' + json.get('auto_increment') if json.get('auto_increment') else ''
    table_collate = 'COLLATE = ' + json.get('collate') if json.get('collate') else ''
    query = 'CREATE TABLE IF NOT EXISTS ' + anonymous_table + '(' + ",".join(enc_columns) + enc_constraint + ')' \
            + engine + table_auto_increment + table_collate + ';'

    table_meta = {
        table_name: {
            'anonymous': anonymous_table,
            'columns': columns_kv,
            'comment': json.get('comment', {}).get('literal')
        }
    }
    Delta().update_delta(db, table_meta)
    return query, {'table': table_name}


def get_enc_columns(columns, columns_info, cipher='symmetric'):
    if not isinstance(columns, list):
        columns = [columns]
    enc_columns = []
    for i in columns:
        feature_info = columns_info.get(i, {})
        enc_feature = i if feature_info.get('plaintext') else feature_info['enc-cols'][cipher]
        enc_columns.append(enc_feature)
    return enc_columns


def get_constraint_key(constraint, columns_info, db=None):
    constraint_name = f"CONSTRAINT {constraint['name']} " if constraint.get('name') else ''
    key_cons = constraint.get('foreign_key', constraint.get('primary_key'))
    assert key_cons is not None, 'constraint only support primary/foreign key'
    reference = key_cons.get('references')
    key = 'FOREIGN KEY' if reference else 'PRIMARY KEY'
    reference_phrase = get_reference_phrase(reference, db) if reference else ''
    columns = key_cons.get('columns')
    enc_columns = get_enc_columns(columns, columns_info)
    key_cons_phrase = constraint_name + key + '(' + ','.join(enc_columns) + ')' + reference_phrase
    return key_cons_phrase


def get_reference_phrase(reference, db):
    table_meta = Delta().meta[db][reference['table']]
    table_name = table_meta['anonymous']
    enc_cols = get_enc_columns(reference['columns'], table_meta['columns'])
    return f" REFERENCES {table_name}({','.join(enc_cols)})"
