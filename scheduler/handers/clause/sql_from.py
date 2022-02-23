from scheduler.handers.clause.rewriter import Rewriter


class SQLFrom(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)

    def rewrite(self, from_value, table, json=None):
        if isinstance(table, list):
            result = []
            for t in table:
                result.append(self.db_meta[t]['anonymous'])
            return result
        if isinstance(table, str):
            return self.db_meta[table]['anonymous']
