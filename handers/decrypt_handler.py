from schema.metadata import CIPHERS


class DecryptHandler:
    def __init__(self, enc_result):
        self.enc_result = enc_result
        self.result = []

    def __repr__(self):
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

