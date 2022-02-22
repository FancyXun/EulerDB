import collections
from scheduler.schema.metadata import FUNC_CIPHERS


class SQLSelect(object):
    def __init__(self, db_meta):
        self.select_columns = collections.OrderedDict()
        self.db_meta = db_meta
        self.select_state = []

    def rewrite(self, select_val, table, cipher='SYMMETRIC', json=None):
        if select_val == "*":
            result = []
            for idx, (k, v) in enumerate(self.db_meta[table]['columns'].items()):
                if v['PLAINTEXT']:
                    result.append(k)
                    self.select_state.append("PLAINTEXT")
                else:
                    result.append(v['ENC_COLUMNS'][cipher])
                    self.select_state.append(cipher)
                self.select_columns[k] = v['TYPE']
            return result
        if select_val == {'count': '*'}:
            self.select_state.append("PLAINTEXT")
            self.select_columns["count"] = "int"
            return select_val
        if isinstance(select_val, list):
            return [self.rewrite(v['value'], table) for v in select_val]
        if isinstance(select_val, str):
            col = self.db_meta[table]['columns'][select_val]
            self.select_columns[select_val] = col['TYPE']
            if col['PLAINTEXT']:
                self.select_state.append("PLAINTEXT")
                return select_val
            else:
                self.select_state.append(cipher)
                return col['ENC_COLUMNS'][cipher]
        if isinstance(select_val, dict):
            result = {}
            for k, v in select_val.items():
                if k in FUNC_CIPHERS.keys():
                    result[k] = self.rewrite(v, table, FUNC_CIPHERS[k])
                else:
                    result[k] = self.rewrite(v, table)
            return result

