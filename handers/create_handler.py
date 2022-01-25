import re

from schema.metadata import Delta


class CreateTableHandler:
    def __init__(self, original_query, parser, db_name):
        self.original_query = original_query
        self.parser = parser
        self.db_name = db_name
        self.__rewrite__()
        self.delta = self.update_delta()

    def __repr__(self):
        pass

    def __rewrite__(self):
        """

        self.query = re.sub(r'varchar\([0-9]+\)', 'varchar(300)',
                            re.sub(r' int ', ' varchar(300) ',
                                   re.sub(r' int,', ' varchar(300),', self.original_query)))
        """
        self.query = re.sub(r'varchar\([0-9]+\)', 'varchar(300)', self.original_query)

    def update_delta(self):
        assert (len(self.parser.tables) == 1)
        return Delta().update_delta(self.db_name, self.parser.tables[0],
                                    dict(zip(self.parser.columns, self.parser.columns_type)))






