from scheduler.handers.clause.rewriter import Rewriter
from scheduler.handers.clause.sql_where import SQLWhere

join_list = ['inner join', 'join', 'left join', 'right join']
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

        if isinstance(table, dict):
            # 可能是select 嵌套, 从table中获取真正的table_name,
            # todo: 如何确定table name和被选中的column之间的关系
            def get_table(k_word, _from):
                if k_word == 'from' and isinstance(_from, str):
                    return _from
                if isinstance(_from, dict):
                    for _k, _v in _from.items():
                        r = get_table(_k, _v)
                        if r is None:
                            continue
                        return r
                return None

            for k, v in table.items():
                table_name = get_table(k, v)
                if table_name is not None:
                    table = table_name
                    break

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
