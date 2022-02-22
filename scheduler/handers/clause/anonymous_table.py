

class AnonymousTable(object):
    def __init__(self, db_meta):
        self.db_meta = db_meta

    def rewrite(self, value, table, json=None):
        return self.db_meta[table]['anonymous']

