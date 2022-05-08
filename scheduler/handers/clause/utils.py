
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


def get_nest_table(table):
    for k, v in table.items():
        table_name = get_table(k, v)
        if table_name is not None:
            table = table_name
            break
    return table