from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor


class DecryptQueryExecutor(AbstractQueryExecutor):
    def __init__(self, handler):
        super().__init__(handler)

    def decrypt(self, executor, query_info, select_columns, db_meta, table):
        result = executor.result
        if result:
            if 'ciphertext' in query_info and query_info['ciphertext']:
                return result
            return self.handler(executor).decrypt(result, select_columns, db_meta, table)
        return []
