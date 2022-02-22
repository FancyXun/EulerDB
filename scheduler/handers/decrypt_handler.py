from scheduler.schema.metadata import CIPHERS_META
from scheduler.handers.base import Handler


class DecryptHandler(Handler):
    def __init__(self, executor, original_query=None, parser=None, db_name=None):
        super().__init__(original_query, parser, db_name)
        self.executor = executor
        self.result = []

    def __repr__(self):
        pass

    def __rewrite__(self):
        pass

    def decrypt(self, enc_result):
        result_state = self.executor.rewriter.select.select_state
        for row in enc_result:
            new_row = []
            for state, col in zip(result_state, row):
                if state == "PLAINTEXT":
                    new_row.append(col)
                else:
                    new_row.append(self.__decrypt__(col))
            self.result.append(tuple(new_row))
        return self.result

    @staticmethod
    def __decrypt__(enc):
        if isinstance(enc, int):
            return CIPHERS_META["OPE"].decrypt(enc)
        return CIPHERS_META["SYMMETRIC"].decrypt(enc)
