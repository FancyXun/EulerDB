from backend.executors.abstract_executor import AbstractQueryExecutor
from schema.encrypt import fernetCipher


class DecryptQueryExecutor(AbstractQueryExecutor):
    def __init__(self, handler=None):
        super().__init__(handler)

    def decrypt(self, result):
        descrypt_result = []
        for row in result:
            new_row = []
            for col in row:
                new_row.append(fernetCipher.decrypt(col))
            descrypt_result.append(new_row)
        print(descrypt_result)