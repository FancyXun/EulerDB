import collections
from scheduler.schema.metadata import FUNC_CIPHERS
from scheduler.handers.clause.rewriter import Rewriter


class SQLSelect(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)
        self.select_columns = collections.OrderedDict()
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
            if isinstance(table, list):
                t_name, col_name = self.split_table_col(select_val, table)
                col = self.db_meta[t_name]['columns'][col_name]
                self.select_columns[select_val] = col['TYPE']
                if col['PLAINTEXT']:
                    self.select_state.append("PLAINTEXT")
                    return self.db_meta[t_name]['anonymous'] + "." + col_name
                else:
                    self.select_state.append(cipher)
                    return self.db_meta[t_name]['anonymous'] + "." + col['ENC_COLUMNS'][cipher]
            else:
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