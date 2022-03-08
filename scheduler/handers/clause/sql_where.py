import logging

from scheduler.crypto import encrypt
from scheduler.utils.keywords import Logical_Operation
from scheduler.handers.clause.rewriter import Rewriter


class SQLWhere(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)

    def rewrite(self, where_value, table, cipher='symmetric', json=None, key=""):
        try:
            col = self.db_meta[table]['columns']
        except Exception as e:
            # table is list
            logging.error(e)
            col = None
        if isinstance(where_value, dict):
            for k, v in where_value.items():
                if k == 'eq':
                    if col:
                        if not col[v[0]]['plaintext']:
                            return {'eq': [col[v[0]]['enc-cols']["symmetric"],
                                           self.rewrite(v[1], table, key=col[v[0]]['key'])]}
                    else:
                        # We don't handle the situation when the join items are plaintext and cipher, but the sql still
                        # works. It just returns empty result...
                        t_name, col_name = self.split_table_col(v[0], table)
                        t_name1, col_name1 = self.split_table_col(v[1], table)
                        col = self.db_meta[t_name]['columns'][col_name]
                        col1 = self.db_meta[t_name1]['columns'][col_name1]
                        col_name = col_name if col['plaintext'] else col['enc-cols'][cipher]
                        col_name1 = col_name1 if col1['plaintext'] else col1['enc-cols'][cipher]
                        return {
                            'eq': [self.db_meta[t_name]['anonymous'] + "." + col_name,
                                   self.db_meta[t_name1]['anonymous'] + "." + col_name1
                                   ]}

                if k in ['gt', 'lt', 'gte', 'lte']:
                    if not col[v[0]]['plaintext']:
                        if col[v[0]]['type'] == 'int':
                            return {k: [col[v[0]]['enc-cols']["order-preserving"],
                                        self.rewrite(v[1], table, cipher='order-preserving', key=col[v[0]]['key'])]}
                if k == 'like':
                    if not col[v[0]]['plaintext']:
                        return {
                            'like': [col[v[0]]['enc-cols']["fuzzy"],
                                     self.rewrite(v[1], table, cipher='fuzzy', key=col[v[0]]['key'])]
                        }
                if k in Logical_Operation:
                    return {k: [self.rewrite(a_v, table, cipher=cipher) for a_v in v]}
                if k == 'literal':
                    return {'literal': self.rewrite(v, table, cipher=cipher, key=key)}
        if isinstance(where_value, int):
            if cipher == 'fuzzy':
                partial_list = str(where_value).split("%")
                result = []
                for partial in partial_list:
                    if partial == "":
                        result.append("")
                        continue
                    else:
                        result.append(encrypt.FuzzyCipher(key).encrypt(str(partial)))
                return "%".join(result)
            if cipher == 'symmetric':
                return encrypt.AESCipher(key).encrypt(str(where_value))
        if isinstance(where_value, str):
            if cipher == 'fuzzy':
                partial_list = where_value.split("%")
                result = []
                for partial in partial_list:
                    if partial == "":
                        result.append("")
                        continue
                    else:
                        result.append(encrypt.FuzzyCipher(key).encrypt(partial))
                return "%".join(result)
            if cipher == 'symmetric':
                return encrypt.AESCipher(key).encrypt(where_value)
        return where_value
