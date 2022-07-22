
def get_table(k_word, _from):
    if k_word in ['from', 'join', 'left join', 'right join', 'inner join', 'value'] and isinstance(_from, str):
        return _from
    if isinstance(_from, dict):
        for _k, _v in _from.items():
            r = get_table(_k, _v)
            if r is None:
                continue
            return r
    return None


def get_nest_table(table):
    for k, v in table.items():
        table_name = get_table(k, v)
        if table_name is not None:
            table = table_name
            break
    return table


def get_origin_table(table_name, table):
    res = ''
    if isinstance(table, dict):
        for key in ['join', 'inner join', 'left join', 'right join']:
            if key in table:
                table = table[key]
        if isinstance(table, str):
            res = get_origin_table(table_name, table)
        if isinstance(table, dict):
            if table.get('name') == table_name:
                res = get_nest_table(table)
    if isinstance(table, list):
        for i in table:
            res_i = get_origin_table(table_name, i)
            res = res_i if res_i else res
    if isinstance(table, str):
        if table_name == table:
            res = table
    return res

