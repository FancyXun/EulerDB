

class OrderBy(object):
    def __init__(self, db_meta):
        self.db_meta = db_meta

    def rewrite(self, orderby_val, table, json=None):
        col = self.db_meta[table]['columns'][orderby_val['value']]
        if col['PLAINTEXT']:
            return orderby_val
        else:
            return {'value': col['ENC_COLUMNS']['OPE']}
