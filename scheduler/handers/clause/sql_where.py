from scheduler.schema.metadata import CIPHERS_META
from scheduler.utils.keywords import Logical_Operation
from scheduler.handers.clause.rewriter import Rewriter


class SQLWhere(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)

    def rewrite(self, where_value, table, cipher='SYMMETRIC', json=None):
        try:
            col = self.db_meta[table]['columns']
        except Exception as e:
            # table is list
            col = None
        if isinstance(where_value, dict):
            for k, v in where_value.items():
                if k == 'eq':
                    if col:
                        if not col[v[0]]['PLAINTEXT']:
                            return {'eq': [col[v[0]]['ENC_COLUMNS']["SYMMETRIC"],
                                           self.rewrite(v[1], table)]}
                    else:
                        # We don't handle the situation when the join items are plaintext and cipher, but the sql still
                        # works. It just returns empty result...
                        t_name, col_name = self.split_table_col(v[0], table)
                        t_name1, col_name1 = self.split_table_col(v[1], table)
                        col = self.db_meta[t_name]['columns'][col_name]
                        col1 = self.db_meta[t_name1]['columns'][col_name1]
                        col_name = col_name if col['PLAINTEXT'] else col['ENC_COLUMNS'][cipher]
                        col_name1 = col_name1 if col1['PLAINTEXT'] else col1['ENC_COLUMNS'][cipher]
                        return {
                            'eq': [self.db_meta[t_name]['anonymous'] + "." + col_name,
                                   self.db_meta[t_name1]['anonymous'] + "." + col_name1
                                   ]}

                if k in ['gt', 'lt', 'gte', 'lte']:
                    if not col[v[0]]['PLAINTEXT']:
                        if col[v[0]]['TYPE'] == 'int':
                            return {k: [col[v[0]]['ENC_COLUMNS']["OPE"],
                                        self.rewrite(v[1], table, 'OPE')]}
                if k == 'like':
                    if not col[v[0]]['PLAINTEXT']:
                        return {
                            'like': [col[v[0]]['ENC_COLUMNS']["FUZZY"],
                                     self.rewrite(v[1], table, 'FUZZY')]
                        }
                if k in Logical_Operation:
                    return {k: [self.rewrite(a_v, table, cipher) for a_v in v]}
                if k == 'literal':
                    return {'literal': self.rewrite(v, table, cipher)}
        if isinstance(where_value, int):
            if cipher == 'FUZZY':
                partial_list = str(where_value).split("%")
                result = []
                for partial in partial_list:
                    if partial == "":
                        result.append("")
                        continue
                    else:
                        result.append(CIPHERS_META[cipher].encrypt(str(partial)))
                return "%".join(result)
            return CIPHERS_META[cipher].encrypt(str(where_value))
        if isinstance(where_value, str):
            if cipher == 'FUZZY':
                partial_list = where_value.split("%")
                result = []
                for partial in partial_list:
                    if partial == "":
                        result.append("")
                        continue
                    else:
                        result.append(CIPHERS_META[cipher].encrypt(partial))
                return "%".join(result)
            return CIPHERS_META[cipher].encrypt(where_value)
        return where_value
