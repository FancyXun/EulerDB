from scheduler.handers.clause.rewriter import Rewriter


class AnonymousTable(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)

    def rewrite(self, value, table, json=None):
        return self.db_meta[table]['anonymous']
