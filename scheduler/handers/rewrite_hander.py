import collections

from sql_parsing import parse_mysql as parse
from sql_parsing import format

from scheduler.utils.keywords import Select_Operation, Modify_Operation
from scheduler.schema.metadata import Delta
from scheduler.schema.metadata import CIPHERS_META
from scheduler.handers.clause.where import Where
from scheduler.handers.clause.set import SQLSet
from scheduler.handers.clause.select import Select
from scheduler.handers.clause.function import SQLFunction
from scheduler.handers.table import rewrite_table


class Rewriter(object):
    def __init__(self, db, encrypted_cols=None):
        self.db = db
        self.db_meta = {}
        self.encrypted_cols = encrypted_cols
        if db in Delta().meta.keys():
            self.db_meta = Delta().meta[db]
        self.where = Where(self.db_meta)
        self.select = Select(self.db_meta)
        self.sql_set = SQLSet(self.db_meta, self.where)
        self.functions = SQLFunction(self.db_meta)

    def rewrite_query(self, query):
        """

        """
        if self.encrypted_cols:
            return rewrite_table(self.db, self.db_meta, query, self.encrypted_cols)
        result = self.encrypt_items(parse(query))
        return format(result)

    def encrypt_items(self, json):
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
                            list(enc_table_meta['columns'][col]['ENC_COLUMNS'].values()))
                json['columns'] = new_columns
                if 'query' in json.keys():
                    values = json['query']['select']
                    new_values = []
                    for col, value in zip(columns, values):
                        if enc_table_meta['columns'][col]['PLAINTEXT']:
                            new_values.append(value)
                        else:
                            for enc in enc_table_meta['columns'][col]['ENC_COLUMNS'].keys():
                                new_values.append(self.encrypt_value(value['value'], enc))
                    json['query']['select'] = new_values
        for keyword in Select_Operation:
            if keyword in json.keys():
                if 'from' in json.keys():
                    select_table = json['from']
                    json[keyword] = self.select.rewrite(json[keyword], select_table)
                    json['from'] = self.db_meta[select_table]['anonymous']
                    if 'where' in json.keys():
                        json['where'] = self.where.rewrite(json['where'], select_table)
                    if 'orderby' in json.keys():
                        json['orderby'] = self.functions.rewrite_orderby(json['orderby'], select_table)
        for keyword in Modify_Operation:
            if keyword in json.keys():
                modify_table = json[keyword]
                json[keyword] = self.db_meta[modify_table]['anonymous']
                if 'where' in json.keys():
                    json['where'] = self.where.rewrite(json['where'], modify_table)
                if 'set' in json.keys():
                    json['set'] = self.sql_set.rewrite(json['set'], modify_table)
        return json

    @staticmethod
    def encrypt_value(value, enc):
        if isinstance(value, int):
            if CIPHERS_META[enc].input == 'INT':
                return {'value': CIPHERS_META[enc].encrypt(int(value))}
            else:
                return {'value': {'literal': CIPHERS_META[enc].encrypt(str(value))}}
        elif isinstance(value, dict):
            return {
                'value':
                    {'literal': CIPHERS_META[enc].encrypt(str(value['literal']))}}
        else:
            raise Exception("Bad value in json {}".format(value))

