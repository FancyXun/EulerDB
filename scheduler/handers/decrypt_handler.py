import datetime

from decimal import Decimal
from scheduler.crypto import encrypt
from scheduler.handers.base import Handler
from scheduler.handers.clause.utils import get_nest_table, get_origin_table


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
                        col_val = eval(col_val.__str__())

                    if isinstance(col_val, datetime.datetime):
                        new_row.append(col_val.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        new_row.append(col_val)
                else:
                    new_row.append(self.__decrypt__(table, col_val, col_name, state))
            self.result.append(tuple(new_row))
        return self.result

    def decrypt1(self, data, select_columns, db_meta, table, select_state):
        self.db_meta = db_meta

        enc_result = data.get("rows", [])
        print(f"enc_data:{enc_result}")
        for row in enc_result:
            new_row = []
            # row1 = row.split(",")
            for col_name, state, col_val in zip(select_columns, select_state, row):
                print("密文解密：" + col_val)
                if state == "plaintext":
                    if isinstance(col_val, Decimal):
                        col_val = eval(col_val.__str__())

                    if isinstance(col_val, datetime.datetime):
                        new_row.append(col_val.strftime("%Y-%m-%d %H:%M:%S"))
                    else:
                        new_row.append(col_val)
                else:
                    new_row.append(self.__decrypt__(table, col_val, col_name, state))
            self.result.append(new_row)
        # print(f"decrypted data:{self.result}")
        return dict(columns=select_columns, rows=self.result)

    def __decrypt__(self, table, col_val, col_name, state):
        if col_val is None:
            return None
        if "." in col_name:
            table_name, col_name = col_name.split(".")
            table = get_origin_table(table_name, table)
        if isinstance(table, dict):
            # 可能是select 嵌套
            table = get_nest_table(table)
        key = self.db_meta[table]['columns'][col_name]['key']  # AES 密钥
        homo_key = self.db_meta[table]['columns'][col_name].get('homomorphic_key')  # 同态加密密钥
        decrypter = {"symmetric": encrypt.AESCipher(key),
                     "order-preserving": encrypt.OPECipher(key),
                     "arithmetic": encrypt.HomomorphicCipher(homo_key)}
        return encrypt.decode(decrypter[state].decrypt(col_val), self.db_meta[table]['columns'][col_name]['type'])
