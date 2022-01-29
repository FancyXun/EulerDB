from scheduler.schema.metadata import Delta


class Handler:
    def __init__(self, original_query, parser, db_name):
        self.original_query = original_query
        self.parser = parser
        self.db_name = db_name
        self.db_meta = Delta().meta
        self.query = None

    def __rewrite__(self):
        raise NotImplementedError

    def check_table(self):
        assert len(self.parser.tables) == 1
        if self.parser.tables[0] not in self.db_meta[self.db_name]["table_kv"].keys():
            raise Exception("NOT FOUND TABLE IN ".format(self.parser.tables[0]))
