from scheduler.schema.metadata import CIPHERS_META
from scheduler.utils.keywords import Logical_Operation


class SQLWhere(object):
    def __init__(self, db_meta):
        self.db_meta = db_meta

    def rewrite(self, where_value, table, cipher='SYMMETRIC', json=None):
        columns_meta = self.db_meta[table]['columns']
        if isinstance(where_value, dict):
            for k, v in where_value.items():
                if k == 'eq':
                    if not columns_meta[v[0]]['PLAINTEXT']:
                        return {'eq': [columns_meta[v[0]]['ENC_COLUMNS']["SYMMETRIC"],
                                       self.rewrite(v[1], table)]}
                if k in ['gt', 'lt', 'gte', 'lte']:
                    if not columns_meta[v[0]]['PLAINTEXT']:
                        if columns_meta[v[0]]['TYPE'] == 'int':
                            return {k: [columns_meta[v[0]]['ENC_COLUMNS']["OPE"],
                                        self.rewrite(v[1], table, 'OPE')]}
                if k == 'like':
                    if not columns_meta[v[0]]['PLAINTEXT']:
                        return {
                            'like': [columns_meta[v[0]]['ENC_COLUMNS']["FUZZY"],
                                     self.rewrite(v[1], table, 'FUZZY')]
                        }
                if k in Logical_Operation:
                    return {k: [self.rewrite(a_v, table, cipher) for a_v in v]}
                if k == 'literal':
                    return {'literal': self.rewrite(v, table, cipher)}
        if isinstance(where_value, int):
            if cipher == 'FUZZY':
                partial_list = str(where_value).split("%")
                result = []
                for partial in partial_list:
                    if partial == "":
                        result.append("")
                        continue
                    else:
                        result.append(CIPHERS_META[cipher].encrypt(str(partial)))
                return "%".join(result)
            return CIPHERS_META[cipher].encrypt(str(where_value))
        if isinstance(where_value, str):
            if cipher == 'FUZZY':
                partial_list = where_value.split("%")
                result = []
                for partial in partial_list:
                    if partial == "":
                        result.append("")
                        continue
                    else:
                        result.append(CIPHERS_META[cipher].encrypt(partial))
                return "%".join(result)
            return CIPHERS_META[cipher].encrypt(where_value)
        return where_value
