from src.schema.metadata import CIPHERS
from src.handers.base import Handler


class DecryptHandler(Handler):
    def __init__(self, enc_result, original_query=None, parser=None, db_name=None):
        super().__init__(original_query, parser, db_name)
        self.enc_result = enc_result
        self.result = []

    def __repr__(self):
        pass

    def __rewrite__(self):
        pass

    def decrypt(self):
        for row in self.enc_result :
            new_row = []
            for col in row:
                new_row.append(self.__decrypt__(col))
            self.result.append(tuple(new_row))
        return self.result

    @staticmethod
    def __decrypt__(enc):
        return CIPHERS["VARCHAR"].decrypt(enc)

