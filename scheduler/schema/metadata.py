import json
import os

from scheduler.crypto import encrypt

META_PATH = '../data/meta.json'

ENCRYPT_SQL_TYPE = {
    "INT":
        {
            "OPE": "INT", "SYMMETRIC": "VARCHAR(300)"
        },
    "VARCHAR":
        {
            "SYMMETRIC": "VARCHAR(300)"
        }
}

CIPHERS = {
    "VARCHAR": encrypt.AESCipher("8888888"),
    "INT": [encrypt.OPECipher(), encrypt.AESCipher("8888888")]
}

CIPHERS_META = {
    "OPE": encrypt.OPECipher(),
    "SYMMETRIC": encrypt.AESCipher("8888888")
}

FUNC_CIPHERS = {
    "max": "OPE",
    "min": "OPE"
}


class Delta(object):
    __instance = None
    meta = None

    def __new__(cls, *args, **kwargs):
        if Delta.__instance is None:
            Delta.__instance = object.__new__(cls, *args, **kwargs)
            cls.meta = cls.load_delta()
        return Delta.__instance

    def update_delta(self, db_name, table_name, anonymous_meta, table_meta):
        if self.meta:
            if db_name not in self.meta.keys():
                # update database
                self.meta[db_name] = {"plain": {table_name["origin"]: table_meta},
                                      "cipher": {table_name["origin"]: anonymous_meta},
                                      "table_kv": {table_name["origin"]: table_name["anonymous"]}
                                      }
            else:
                self.meta[db_name]["plain"].update({table_name["origin"]: table_meta})
                self.meta[db_name]["cipher"].update({table_name["origin"]: anonymous_meta})
                self.meta[db_name]["table_kv"].update({table_name["origin"]: table_name["anonymous"]})

        else:
            self.meta = {
                db_name:
                    {"plain": {table_name["origin"]: table_meta},
                     "cipher": {table_name["origin"]: anonymous_meta},
                     "table_kv": {table_name["origin"]: table_name["anonymous"]}
                     }
            }
        return self.meta

    def delete_delta(self):
        pass

    def save_delta(self):
        with open(META_PATH, "w") as f:
            json.dump(self.meta, f)

    @staticmethod
    def load_delta():
        if not os.path.exists(META_PATH):
            return {}
        with open(META_PATH, "r") as f:
            meta = json.load(f)
        return meta
