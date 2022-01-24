from schema.metadata import Delta


class DecryptHandler:
    def __init__(self, enc_result):
        self.enc_result = enc_result
        self.result = []
        self.crypto_meta = Delta().keys

    def __repr__(self):
        pass

    def decrypt(self):
        for row in self.enc_result :
            new_row = []
            for col in row:
                new_row.append(self.crypto_meta["varchar"].decrypt(col))
            self.result.append(tuple(new_row))
        return self.result

