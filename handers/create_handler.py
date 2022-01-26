import time,hashlib

import sqlparse
from schema.metadata import Delta, ENCRYPT_SQL_TYPE


class CreateTableHandler:
    def __init__(self, original_query, parser, db_name):
        self.original_query = original_query
        self.parser = parser
        self.db_name = db_name
        self.__rewrite__()

    def __repr__(self):
        pass

    def __rewrite__(self):
        """

        self.query = re.sub(r'varchar\([0-9]+\)', 'varchar(300)',
                            re.sub(r' int ', ' varchar(300) ',
                                   re.sub(r' int,', ' varchar(300),', self.original_query)))
        """
        original_statements = sqlparse.split(self.original_query)
        for stat in original_statements:
            parsed = sqlparse.parse(stat)[0]
            # check statements is a creating table statements
            self.__assert_create_table__(parsed.tokens)
        columns = []
        anonymous_meta = {}
        for col, col_type in zip(self.parser.columns, self.parser.columns_type):
            val = ENCRYPT_SQL_TYPE.get(col_type.upper())
            anonymous_meta[col] = {}
            for key in val.keys():
                col_name = col.upper() + key
                columns.append(col_name + ' ' + val.get(key))
                anonymous_meta[col][key] = col_name
        self.anonymous_table = "TABEL_" + hashlib.md5(str(time.clock()).encode('utf-8')).hexdigest()
        self.query = 'CREATE TABLE IF NOT EXISTS ' + self.anonymous_table + '(' + ",".join(columns) + ');'
        self.__update_delta__(anonymous_meta)

    @staticmethod
    def __assert_create_table__(tokens):
        keywords = []
        for token in tokens:
            if str(token.ttype) == "Token.Keyword.DDL" and token.value.upper() == 'CREATE':
                keywords.append(token.value)
            if str(token.ttype) == "Token.Keyword" and token.value.upper() == 'TABLE':
                keywords.append(token.value)
        assert len(keywords) == 2

    def __update_delta__(self, anonymous_meta):
        assert (len(self.parser.tables) == 1)
        table_name = {
            "origin": self.parser.tables[0],
            "anonymous": self.anonymous_table
        }
        return Delta().update_delta(self.db_name, table_name, anonymous_meta,
                                    dict(zip(self.parser.columns, self.parser.columns_type)))






