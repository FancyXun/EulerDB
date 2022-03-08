from scheduler.handers.clause.rewriter import Rewriter
from scheduler.crypto import encrypt


class SQLValues(Rewriter):
    def __init__(self, db_meta):
        super().__init__(db_meta)

    def rewrite(self, values, table, json=None):
        new_values = []
        enc_table_meta = self.db_meta[table]
        for col, value in zip(json['columns'], values):
            if enc_table_meta['columns'][col]['plaintext']:
                new_values.append(value)
            else:
                for enc in enc_table_meta['columns'][col]['enc-cols'].keys():
                    new_values.append(self.encrypt_value(table, col, value['value'], enc))
        return new_values

    def encrypt_value(self, table, col, value, enc):
        key = self.db_meta[table]['columns'][col]['key']
        if isinstance(value, dict):
            value = value['literal']
        if enc == "order-preserving":
            return {'value': encrypt.OPECipher(key).encrypt(int(value))}
        if enc == "symmetric":
            return {'value': {'literal': encrypt.AESCipher(key).encrypt(str(value))}}
        if enc == "fuzzy":
            return {'value': {'literal': encrypt.FuzzyCipher(key).encrypt(str(value))}}
