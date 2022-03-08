

class Handler:
    def __init__(self, original_query, parser, db_name):
        self.original_query = original_query
        self.parser = parser
        self.db_name = db_name
        self.db_meta = None

    def __rewrite__(self):
        raise NotImplementedError

