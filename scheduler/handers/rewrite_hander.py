from sql_parsing import parse_mysql as parse
from sql_parsing import format
from scheduler.schema.metadata import Delta, CIPHERS_META, FUNC_CIPHERS


class Rewriter(object):
    def __init__(self, db):
        self.db = db
        self.db_meta = Delta().meta[db]

    def rewrite_query(self, query):
        """

        """
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
            json['insert'] = self.db_meta ['table_kv'][insert_table]
            if 'columns' in json.keys():
                columns = json['columns']
                new_columns = []
                for col in columns:
                    new_columns.extend(list(self.db_meta ['cipher'][insert_table][col].values()))
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

