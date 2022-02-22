from scheduler.handers.clause.where import Where


class SQLSet(object):
    def __init__(self, db_meta, where):
        self.db_meta = db_meta
        self.where = where

    def rewrite(self, json, table):
        result = {}
        enc_table_meta = self.db_meta[table]
        for k, v in json.items():
            if enc_table_meta['columns'][k]['PLAINTEXT']:
                result[k] = v
            else:
                for k1, v1 in enc_table_meta['columns'][k]['ENC_COLUMNS'].items():
                    result[v1] = self.where.rewrite(v, table, k1)
        return result
