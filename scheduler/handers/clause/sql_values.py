from scheduler.schema.metadata import CIPHERS_META


class SQLValues(object):
    def __init__(self, db_meta):
        self.db_meta = db_meta

    def rewrite(self, values, table, json=None):
        new_values = []
        enc_table_meta = self.db_meta[table]
        for col, value in zip(json['columns'], values):
            if enc_table_meta['columns'][col]['PLAINTEXT']:
                new_values.append(value)
            else:
                for enc in enc_table_meta['columns'][col]['ENC_COLUMNS'].keys():
                    new_values.append(self.encrypt_value(value['value'], enc))
        return new_values

    @staticmethod
    def encrypt_value(value, enc):
        if isinstance(value, int):
            if CIPHERS_META[enc].input == 'INT':
                return {'value': CIPHERS_META[enc].encrypt(int(value))}
            else:
                return {'value': {'literal': CIPHERS_META[enc].encrypt(str(value))}}
        elif isinstance(value, dict):
            return {
                'value':
                    {'literal': CIPHERS_META[enc].encrypt(str(value['literal']))}}
        else:
            raise Exception("Bad value in json {}".format(value))