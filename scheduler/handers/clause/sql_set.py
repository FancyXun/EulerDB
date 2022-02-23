from scheduler.handers.clause.sql_where import SQLWhere
from scheduler.handers.clause.rewriter import Rewriter


class SQLSet(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)
        self.where = SQLWhere(db_meta)

    def rewrite(self, set_value, table, json=None):
        result = {}
        enc_table_meta = self.db_meta[table]
        for k, v in set_value.items():
            if enc_table_meta['columns'][k]['PLAINTEXT']:
                result[k] = v
            else:
                for k1, v1 in enc_table_meta['columns'][k]['ENC_COLUMNS'].items():
                    result[v1] = self.where.rewrite(v, table, k1)
        return result
