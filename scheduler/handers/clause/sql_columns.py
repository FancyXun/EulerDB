from scheduler.handers.clause.rewriter import Rewriter


class SQLColumns(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)

    def rewrite(self, columns, table, json=None):
        enc_table_meta = self.db_meta[table]
        new_columns = []
        for col in columns:
            if enc_table_meta['columns'][col]['plaintext']:
                new_columns.append(col)
            else:
                new_columns.extend(
                    list(enc_table_meta['columns'][col]['enc-cols'].values()))
        return new_columns
