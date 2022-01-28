from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor


class DecryptQueryExecutor(AbstractQueryExecutor):
    def __init__(self, handler):
        super().__init__(handler)

    def decrypt(self, result):
        if result:
            return self.handler(result).decrypt()
        return None
