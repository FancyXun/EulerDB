from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor


class DecryptQueryExecutor(AbstractQueryExecutor):
    def __init__(self, handler):
        super().__init__(handler)

    def decrypt(self, executor):
        result = executor.result
        if result:
            return self.handler(executor).decrypt(result)
        return []
