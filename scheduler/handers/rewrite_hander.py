import time
import hashlib

from sql_parsing import parse_mysql as parse
from sql_parsing import format
from scheduler.schema.metadata import Delta
from scheduler.schema.metadata import  \
    CIPHERS_META, \
    FUNC_CIPHERS, \
    ENCRYPT_SQL_TYPE, \
    FUZZY_TYPE


class Rewriter(object):
    def __init__(self, db, encrypted_cols=None):
        self.db = db
        self.db_meta = {}
        self.encrypted_cols = encrypted_cols
        if db in Delta().meta.keys():
            self.db_meta = Delta().meta[db]

    def rewrite_query(self, query):
        """

        """
        if self.encrypted_cols:
            return self.rewrite_table(query)
        result = self.encry_items(parse(query))
        return format(result)

    def encry_items(self, json):
        """
        insert:
        {'columns': ['id', 'name', 'age', 'sex', 'score', 'nick_name', 'comment'],
        'query': {
            'select': [{'value': 6190}, {'value': {'literal': 'cmjbh'}}, {'value': 19}, {'value': {'literal': 'm'}},
                       {'value': 86}, {'value': {'literal': 'kmswhvibct'}},
                       {'value': {'literal': 'bmadwsfkshtshjbfgghurgigplgvsg'}}]},
        'insert': 'user'}
        select:
            {'select': '*', 'from': 'user'}
            {'select': [{'value': 'id'}, {'value': 'name'}], 'from': 'user', 'limit': 10}
        """
        if 'insert' in json.keys():
            insert_table = json['insert']
            enc_table_meta = self.db_meta[insert_table]
            json['insert'] = enc_table_meta['anonymous']
            if 'columns' in json.keys():
                columns = json['columns']
                new_columns = []
                for col in columns:
                    if enc_table_meta['columns'][col]['PLAINTEXT']:
                        new_columns.append(col)
                    else:
                        new_columns.extend(
                            list(enc_table_meta['columns'][col]['ENC_COLUMNS'].keys()))
                json['columns'] = new_columns
                if 'query' in json.keys():
                    values = json['query']['select']
                    new_values = []
                    for value in values:
                        new_values.extend(self.encrypt_value(value['value']))
                    json['query']['select'] = new_values
        if 'select' in json.keys():
            if 'from' in json.keys():
                select_table = json['from']
                json['select'] = self.rewrite_select_items(json['select'], select_table)
                json['from'] = self.db_meta['table_kv'][select_table]
                if 'where' in json.keys():
                    json['where'] = self.rewrite_where(json['where'], select_table)
                if 'orderby' in json.keys():
                    json['orderby'] = self.rewrite_orderby(json['orderby'], select_table)
        return json

    @staticmethod
    def encrypt_value(value):
        if isinstance(value, int):
            return [{'value': CIPHERS_META["OPE"].encrypt(int(value))},
                    {'value': {'literal': CIPHERS_META["SYMMETRIC"].encrypt(str(value))}}]
        elif isinstance(value, dict):
            return [{
                'value':
                    {'literal':
                         CIPHERS_META["SYMMETRIC"].encrypt(str(value['literal']))}}]
        else:
            raise Exception("Bad value in json {}".format(value))

    def rewrite_where(self, json, table):
        columns_meta = self.db_meta['cipher'][table]
        if isinstance(json, dict):
            for k, v in json.items():
                if k == 'eq':
                    return {'eq': [columns_meta[v[0]]["SYMMETRIC"],
                                   self.rewrite_where(v[1], table)]}
                if k == 'and':
                    return {'and': [self.rewrite_where(a_v, table) for a_v in v]}
                if k == 'literal':
                    return {'literal': self.rewrite_where(v, table)}
        if isinstance(json, int):
            return CIPHERS_META["SYMMETRIC"].encrypt(str(json))
        if isinstance(json, str):
            return CIPHERS_META["SYMMETRIC"].encrypt(json)
        return json

    def rewrite_select_items(self, json, table, cipher='SYMMETRIC'):
        if json == "*":
            return [v['SYMMETRIC'] for _, v in self.db_meta['cipher'][table].items()]
        if isinstance(json, list):
            return [self.rewrite_select_items(v['value'], table) for v in json]
        if isinstance(json, str):
            return self.db_meta['cipher'][table][json][cipher]
        if isinstance(json, dict):
            result = {}
            for k, v in json.items():
                if k in FUNC_CIPHERS.keys():
                    result[k] = self.rewrite_select_items(v, table, FUNC_CIPHERS[k])
                else:
                    result[k] = self.rewrite_select_items(v, table)
            return result

    def rewrite_orderby(self, json, table):
        return {'value': self.db_meta['cipher'][table][json['value']]['OPE']}

    def rewrite_table(self, query):
        json = parse(query)
        if 'create table' not in json.keys():
            raise Exception("Bad create table query: {}".format(query))
        create_table = json['create table']
        table_name = create_table['name']
        columns = create_table['columns']
        enc_columns = []
        columns_kv = {}
        for col in columns:
            col_name = col['name']
            col_type = col['type']
            columns_kv[col_name] = {}
            columns_kv[col_name]['ENC_COLUMNS'] = {}
            if col['name'] not in self.encrypted_cols:
                for t_k, t_v in col_type.items():
                    if t_v:
                        enc_col_type = t_k + '(' + str(t_v) + ')'
                    else:
                        enc_col_type = t_k
                    enc_columns.append(col_name + ' ' + enc_col_type)
                    columns_kv[col_name]['PLAINTEXT'] = True
                continue
            val = ENCRYPT_SQL_TYPE.get(list(col['type'].keys())[0].upper())
            for k, v in val.items():
                enc_col_name = col_name.upper() + k
                for t_k, t_v in col_type.items():
                    if t_v:
                        enc_col_type = t_k + '(' + str(t_v * v) + ')'
                    else:
                        enc_col_type = v
                    enc_columns.append(enc_col_name + ' ' + enc_col_type)
                    columns_kv[col_name]['ENC_COLUMNS'].update({enc_col_name: k})
            if self.encrypted_cols[col['name']]['fuzzy']:
                enc_col_name = col_name.upper() + 'FUZZY'
                enc_columns.append(enc_col_name + ' ' + FUZZY_TYPE)
                columns_kv[col_name]['ENC_COLUMNS'].update({enc_col_name: ""})
            columns_kv[col_name]['PLAINTEXT'] = False
        anonymous_table = "TABEL_" + hashlib.md5(str(time.clock()).encode('utf-8')).hexdigest()
        query = 'CREATE TABLE ' + anonymous_table + '(' + ",".join(enc_columns) + ');'
        table_meta = {
            table_name: {
                'anonymous': anonymous_table,
                'columns': columns_kv
            }
        }
        Delta().update_delta(self.db, table_meta)
        return query

