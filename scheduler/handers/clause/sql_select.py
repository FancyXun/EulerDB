from scheduler.handers.clause.utils import get_nest_table, get_origin_table
from scheduler.schema.metadata import FUNC_CIPHERS
from scheduler.handers.clause.rewriter import Rewriter
from scheduler.handers.clause.sql_from import join_list


class SQLSelect(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)
        self.select_columns = []
        self.select_types = []
        self.select_state = []

    def rewrite(self, select_val, table, cipher='symmetric', json=None):
        # if isinstance(table, dict):
        #     # 可能是select 嵌套, 从table中获取真正的table_name,
        #     # todo: 如何确定table name和被选中的column之间的关系
        #     table = get_nest_table(table)

        if select_val == "*":
            result = []
            cache_table = table if isinstance(table, list) else [table]
            tables = [get_nest_table(i) if isinstance(i, dict) else i for i in cache_table]
            for i, single_table in enumerate(tables):
                # table_col = self.get_items_from_table(single_table, cache_table[i])
                for idx, (k, v) in enumerate(self.db_meta[single_table]['columns'].items()):
                    ti = cache_table[i]
                    if isinstance(ti, dict):
                        for key in join_list:
                            if key in ti:
                                ti = ti[key]
                        if isinstance(ti, dict):
                            name = ti.get('name')
                            if len(cache_table) > 1:
                                associate_t_name = name if name else self.db_meta[single_table]['anonymous']
                            else:
                                associate_t_name = name if name else ''
                        else:
                            associate_t_name = self.db_meta[ti]['anonymous']
                    else:
                        associate_t_name = self.db_meta[single_table]['anonymous'] if len(cache_table) > 1 else ''
                    associate_t_name = associate_t_name + '.' if associate_t_name else associate_t_name
                    if v['plaintext']:
                        result.append(associate_t_name + k)
                        self.select_state.append("plaintext")
                    else:
                        result.append(associate_t_name + v['enc-cols'][cipher])
                        self.select_state.append(cipher)
                    select_column_name = single_table + '.' if len(cache_table) > 1 else ''
                    self.select_columns.append(select_column_name + k)
                    self.select_types.append(v['type'])
            return result
        if select_val == {'count': '*'}:
            self.select_state.append("plaintext")
            self.select_columns.append("count")
            self.select_types.append("int")
            return select_val
        if isinstance(select_val, list):
            return [self.rewrite(v['value'], table) for v in select_val]
        if isinstance(select_val, str):
            self.select_columns.append(select_val)
            if isinstance(table, list):
                # todo modify this
                t_name, col_name = self.split_table_col(select_val, table)
                true_table_name = get_origin_table(t_name, table)
                col = self.db_meta[true_table_name]['columns'][col_name]
                t_name = t_name if t_name != true_table_name else self.db_meta[true_table_name]['anonymous'] + "."
                self.select_types.append(col['type'])
                if col['plaintext']:
                    self.select_state.append("plaintext")
                    return t_name + col_name
                else:
                    self.select_state.append(cipher)
                    return t_name + col['enc-cols'][cipher]
            else:
                if isinstance(table, str):  # which means table is true table name or some nest table without join
                    t_name, select_val = select_val.split('.') if '.' in select_val else ('',  select_val)
                    col = self.db_meta[table]['columns'][select_val]
                    t_name = t_name + '.' if t_name else ''
                    self.select_types.append(col['type'])
                    if col['plaintext']:
                        self.select_state.append("plaintext")
                        return t_name + select_val
                    else:
                        self.select_state.append(cipher)
                        return t_name + col['enc-cols'][cipher]
                else:
                    if isinstance(table, dict):
                        # 可能是select 嵌套, 从table中获取真正的table_name,
                        # todo: 如何确定table name和被选中的column之间的关系
                        table = get_nest_table(table)
                    t_name, select_val = select_val.split('.') if '.' in select_val else ('',  select_val)
                    col = self.db_meta[table]['columns'][select_val]
                    t_name = t_name + '.' if t_name else ''
                    self.select_types.append(col['type'])
                    if col['plaintext']:
                        self.select_state.append("plaintext")
                        return t_name + select_val
                    else:
                        self.select_state.append(cipher)
                        return t_name + col['enc-cols'][cipher]

        if isinstance(select_val, dict):
            result = {}
            for k, v in select_val.items():
                if k in FUNC_CIPHERS.keys():
                    result[k] = self.rewrite(v, table, FUNC_CIPHERS[k])
                elif k == 'add':
                    v0, v1 = self.rewrite(v[0], table), self.rewrite(v[1], table)
                    cipher_text = self.select_state.pop()
                    self.select_types.pop()
                    assert self.select_state[-1] == self.select_state[-2] and \
                           cipher_text in ['plaintext', 'arithmetic'], f'{v[0]}+{v[1]} not allowed'

                    if cipher_text == 'plaintext':
                        result[k] = [v0, v1]
                    if cipher_text == 'arithmetic':
                        if '.' in v0:
                            t0, col0 = v0.split('.')
                            t0 = get_origin_table(t0, table)
                            p0 = self.db_meta[t0]['columns'][col0]['homomorphic_key']
                        else:
                            p0 = self.db_meta[table]['columns'][v0]['homomorphic_key']
                        if '.' in v1:
                            t1, col1 = v1.split('.')
                            t1 = get_origin_table(t1, table)
                            p1 = self.db_meta[t1]['columns'][col1]['homomorphic_key']
                        else:
                            p1 = self.db_meta[table]['columns'][v1]['homomorphic_key']
                        assert p0 == p1, 'two columns to be added need to have the same key'
                        p = (p1[0] * p1[0]) ** 2
                        result['mod'] = [{'mul': [v0, v1]}, p]
                else:
                    result[k] = self.rewrite(v, table)
            return result
