from scheduler.compile import parse
from scheduler.handers.base import Handler
from scheduler.schema.metadata import Delta, CIPHERS_META


class InsertHandler(Handler):
    def __init__(self, original_query, parser, db_name):
        super().__init__(original_query, parser, db_name)
        self.query = None
        self.__rewrite__()

    def __repr__(self):
        pass

    def __rewrite__(self):
        """

        """

        original_statements = parse.split(self.original_query)
        statements = []
        for stat in original_statements:
            parsed = parse.parse(stat)[0]
            statements.append(parsed.__str__())
        self.check_table()
        self.ele_rewrite()

    def ele_rewrite(self):
        anonymous_table = self.db_meta["table_kv"][self.db_name][self.parser.tables[0]]
        columns = []
        values = []
        for col, val in zip(self.parser.columns, self.parser.values):
            columns_meta = self.db_meta["cipher"][self.db_name][anonymous_table][col]
            for k, v in columns_meta.items():
                if CIPHERS_META[k].input == 'INT':
                    values.append(str(CIPHERS_META[k].encrypt(int(val))))
                else:
                    values.append("'" + CIPHERS_META[k].encrypt(str(val)) + "'")
                columns.append(v)
        self.query = "INSERT INTO " + anonymous_table + "(" + ",".join(columns) + ")" + " VALUES " \
                     + "(" + ",".join(values) + ");"







