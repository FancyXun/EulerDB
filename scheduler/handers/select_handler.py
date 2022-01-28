from scheduler.compile import parse
from scheduler.handers.base import Handler


class SelectHandler(Handler):
    def __init__(self, original_query, parser, db_name):
        super().__init__(original_query, parser, db_name)
        self.select_ast = SelectAST()
        self.__rewrite__()

    def __repr__(self):
        pass

    def __rewrite__(self):
        original_statements = parse.split(self.original_query)
        statements = []
        for stat in original_statements:
            parsed = parse.parse(stat)[0]
            statements.append(parsed.__str__())
        self.check_table()
        self.select_items()

    def select_items(self):
        columns = self.parser.columns
        anonymous_table = self.db_meta["table_kv"][self.db_name][self.parser.tables[0]]
        if columns == ['*']:
            for _, item in self.db_meta["cipher"][self.db_name][anonymous_table].items():
                for k, v in item.items():
                    if k == "SYMMETRIC":
                        self.select_ast.items.append(v)
        self.select_ast.tables.append(anonymous_table)
        self.rewrite_sql()

    def rewrite_sql(self):
        items = self.select_ast.items
        table = self.select_ast.tables[0]
        self.query = "SELECT " + ",".join(items) + " FROM " + table


class SelectAST:
    def __init__(self):
        self.items = []
        self.tables = []
        self.where = []
