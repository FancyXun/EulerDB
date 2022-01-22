import sqlparse


class SelectHandler:
    def __init__(self, original_query):
        self.original_query = original_query
        self.__rewrite__()

    def __repr__(self):
        pass

    def __rewrite__(self):
        original_statements = sqlparse.split(self.original_query)
        statements = []
        for stat in original_statements:
            parsed = sqlparse.parse(stat)[0]
            statements.append(parsed.__str__())
        self.query = ";".join(statements)


