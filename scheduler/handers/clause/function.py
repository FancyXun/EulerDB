

class SQLFunction(object):
    def __init__(self, db_meta):
        self.db_meta = db_meta

    def rewrite_orderby(self, json, table):
        col = self.db_meta[table]['columns'][json['value']]
        if col['PLAINTEXT']:
            return json
        else:
            return {'value': col['ENC_COLUMNS']['OPE']}