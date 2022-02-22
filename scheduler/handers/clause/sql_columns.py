

class SQLColumns(object):
    def __init__(self, db_meta):
        self.db_meta = db_meta

    def rewrite(self, columns, table, json=None):
        enc_table_meta = self.db_meta[table]
        new_columns = []
        for col in columns:
            if enc_table_meta['columns'][col]['PLAINTEXT']:
                new_columns.append(col)
            else:
                new_columns.extend(
                    list(enc_table_meta['columns'][col]['ENC_COLUMNS'].values()))
        return new_columns
