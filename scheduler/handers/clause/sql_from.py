from scheduler.handers.clause.rewriter import Rewriter
from scheduler.handers.clause.sql_where import SQLWhere

join_list = ['inner join']
on_list = ['on']


class SQLFrom(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)
        self.sql_where = SQLWhere(db_meta)

    def rewrite(self, from_value, table, json=None):
        if isinstance(table, list):
            result = []
            for t in table:
                if isinstance(t, str):
                    result.append(self.db_meta[t]['anonymous'])
                else:
                    result.append(self.join(t))
            return result
        if isinstance(table, str):
            return self.db_meta[table]['anonymous']

    def join(self, t_value):
        if isinstance(t_value, dict):
            result = {}
            for key, value in t_value.items():
                if key in join_list:
                    result[key] = self.db_meta[value]['anonymous']
                if key in on_list:
                    result[key] = self.sql_where.rewrite(value, [])
        else:
             raise Exception("un support sql json in join: {}".format(t_value))
        return result
