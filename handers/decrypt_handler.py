from schema.metadata import Delta


class DecryptHandler:
    def __init__(self, enc_result):
        self.enc_result = enc_result
        self.result = []
        self.ciphers = Delta().ciphers

    def __repr__(self):
        pass

    def decrypt(self):
        for row in self.enc_result :
            new_row = []
            for col in row:
                new_row.append(self.__decrypt__(col))
            self.result.append(tuple(new_row))
        return self.result

    def __decrypt__(self, enc):
        if isinstance(enc, int):
            return self.ciphers["int"].decrypt(enc)

        return self.ciphers["varchar"].decrypt(enc)

