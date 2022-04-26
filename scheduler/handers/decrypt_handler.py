import time

from decimal import Decimal
from scheduler.crypto import encrypt
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

    def decrypt(self, enc_result, select_columns, db_meta, table):
        self.db_meta = db_meta
        result_state = self.executor.rewriter.select.select_state
        for row in enc_result:
            new_row = []
            for col_name, state, col_val in zip(select_columns, result_state, row):
                if state == "plaintext":
                    if isinstance(col_val, Decimal):
                        col_val = float(col_val)
                    new_row.append(col_val)
                else:
                    new_row.append(self.__decrypt__(table, col_val, col_name, state))
            self.result.append(tuple(new_row))
        return self.result

    def __decrypt__(self, table, col_val, col_name, state):
        if col_val is None:
            return None
        if "." in col_name:
            table, col_name = col_name.split(".")
        key = self.db_meta[table]['columns'][col_name]['key']
        homo_key = self.db_meta[table]['columns'][col_name].get('homomorphic_key')
        decrypter = {"symmetric": encrypt.AESCipher(key),
                     "order-preserving": encrypt.OPECipher(key),
                     "arithmetic": encrypt.HomomorphicCipher(homo_key)}
        return encrypt.decode(decrypter[state].decrypt(col_val), self.db_meta[table]['columns'][col_name]['type'])
