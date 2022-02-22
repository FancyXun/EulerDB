import collections
from scheduler.schema.metadata import FUNC_CIPHERS


class Select(object):
    def __init__(self, db_meta):
        self.select_columns = collections.OrderedDict()
        self.db_meta = db_meta
        self.select_state = []

    def rewrite(self, json, table, cipher='SYMMETRIC'):
        if json == "*":
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
        if json == {'count': '*'}:
            self.select_state.append("PLAINTEXT")
            self.select_columns["count"] = "int"
            return json
        if isinstance(json, list):
            return [self.rewrite(v['value'], table) for v in json]
        if isinstance(json, str):
            col = self.db_meta[table]['columns'][json]
            self.select_columns[json] = col['TYPE']
            if col['PLAINTEXT']:
                self.select_state.append("PLAINTEXT")
                return json
            else:
                self.select_state.append(cipher)
                return col['ENC_COLUMNS'][cipher]
        if isinstance(json, dict):
            result = {}
            for k, v in json.items():
                if k in FUNC_CIPHERS.keys():
                    result[k] = self.rewrite(v, table, FUNC_CIPHERS[k])
                else:
                    result[k] = self.rewrite(v, table)
            return result

