import re


class CreateTableHandler:
    def __init__(self, original_query):
        self.original_query = original_query
        self.__rewrite__()
        pass

    def __repr__(self):
        pass

    def __rewrite__(self):

        self.query = re.sub(r'varchar\([0-9]+\)', 'varchar(300)',
                            re.sub(r' int ', ' varchar(300) ',
                                   re.sub(r' int,', ' varchar(300),', self.original_query)))


