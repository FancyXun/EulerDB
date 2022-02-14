from mysql.connector import Error
from scheduler.handers.rewrite_hander import Rewriter
from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor
from scheduler.compile.keywords_lists import QueryType
from scheduler.schema.metadata import Delta

SQL_TYPES = [
    QueryType.CREATE, QueryType.SELECT, QueryType.INSERT, QueryType.DELETE, QueryType.UPDATE
]


class RemoteExecutor(AbstractQueryExecutor):
    def __init__(self, conn):
        super().__init__(handler=None)
        self.conn = conn
        self.result = None
        self.encrypted_cols = None
        self.rewriter = None

    def call(self, query, parser, encrypted_cols):
        """

        """
        query_type = parser.query_type
        self.encrypted_cols = encrypted_cols
        if query_type not in SQL_TYPES:
            raise NotImplementedError("Not support {} sql type".format(query_type))
        if self.encrypted_cols:
            if parser.query_type == QueryType.CREATE:
                enc_query = self.dispatch(query, self.conn.database, self.encrypted_cols)
                if self.conn.is_connected():
                    try:
                        cursor = self.conn.cursor()
                        cursor.execute(enc_query)
                        Delta().save_delta()
                    except Error as e:
                        print("Error while connecting to MySQL", e)
                        # todo:delete tabel meta
        else:
            if parser.query_type == QueryType.SELECT:
                enc_query = self.dispatch(query, self.conn.database, self.encrypted_cols)
                if self.conn.is_connected():
                    cursor = self.conn.cursor()
                    cursor.execute(enc_query)
                    self.result = cursor.fetchall()
            else:
                enc_query = self.dispatch(query, self.conn.database, self.encrypted_cols)
                if self.conn.is_connected():
                    cursor = self.conn.cursor()
                    cursor.execute(enc_query)
                    self.conn.commit()

    def dispatch(self, query, db, enc_cols):
        self.rewriter = Rewriter(db, enc_cols)
        return self.rewriter.rewrite_query(query)

    def get_sql_columns(self):
        return self.rewriter.select_columns


