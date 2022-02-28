from scheduler.handers.clause.rewriter import Rewriter


class SQLDrop(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)

    def rewrite(self, columns, table, json=None):
        result = {}
        for key, value in columns.items():
            result[key] = self.db_meta[value]['anonymous']
        return result
