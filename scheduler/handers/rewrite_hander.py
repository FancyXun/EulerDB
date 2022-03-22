import copy

from sql_parsing import parse_mysql as parse
from sql_parsing import format, parse_alter_key, format_alter_key

from scheduler.handers.table import rewrite_table
from scheduler.handers.clause.sql_clause import Clause
from scheduler.handers.clause import clause
from scheduler.handers.clause import table_key

from scheduler.schema.metadata import Delta


class Rewriter(Clause):
    def __init__(self, db, encrypted_cols=None):
        super().__init__(db, encrypted_cols)

    def rewrite_query(self, query):
        """

        """
        table = None
        self.origin_query = query
        if self.encrypted_cols:
            return rewrite_table(self.db, self.db_meta, query, self.encrypted_cols)
        alter_key_json = parse_alter_key(query)
        json = parse(query) if not alter_key_json else alter_key_json
        source_json = copy.deepcopy(json)
        for key in table_key:
            if key in json.keys():
                table = json[key]
                break
        else:
            raise Exception("no table found in sql {}".format(self.origin_query))

        def __inner__(inner_json, inner_key, _func):
            if isinstance(inner_json, dict):
                if isinstance(inner_json[inner_key], dict):
                    for _k, _v in inner_json[inner_key].items():
                        tmp = __inner__(_v, _k, _func)
                        inner_json[inner_key][_k] = tmp
                    return inner_json
            else:
                return func[inner_key].rewrite(inner_json, table, json=source_json)

        for key in clause.keys():
            if key not in json.keys():
                continue
            func = self.__getattribute__(key)
            if isinstance(func, dict):
                json = __inner__(json, key, func)
            else:
                json[key] = func.rewrite(json[key], table, json=source_json)
        # record json in Delta
        Delta.table_json = json
        if 'alter' in json:
            return format_alter_key(json), table
        return format(json), table
