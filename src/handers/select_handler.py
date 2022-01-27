from src.compile import parse
from src.handers.base import Handler


class SelectHandler(Handler):
    def __init__(self, original_query, parser, db_name):
        super().__init__(original_query, parser, db_name)
        self.__rewrite__()

    def __repr__(self):
        pass

    def __rewrite__(self):
        original_statements = parse.split(self.original_query)
        statements = []
        for stat in original_statements:
            parsed = parse.parse(stat)[0]
            statements.append(parsed.__str__())
        self.query = ";".join(statements)


