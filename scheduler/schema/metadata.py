import json
import os

from scheduler.crypto import encrypt

META_PATH = 'data/meta.json'

ENCRYPT_SQL_TYPE = {
    "INT":
        {
            "OPE": "BIGINT",
            "SYMMETRIC": "VARCHAR(300)"
        },
    "VARCHAR":
        {
            "SYMMETRIC": 20
        }
}

FUZZY_TYPE = 'VARCHAR(2000)'

CIPHERS = {
    "VARCHAR": encrypt.AESCipher("points"),
    "INT": [encrypt.OPECipher(), encrypt.AESCipher("points")]
}

CIPHERS_META = {
    "OPE": encrypt.OPECipher(),
    "SYMMETRIC": encrypt.AESCipher("points"),
    "FUZZY": encrypt.FuzzyCipher()
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

    def update_delta(self, db_name, table_meta):
        if self.meta:
            if db_name not in self.meta.keys():
                self.meta.update({db_name: table_meta})
            else:
                self.meta[db_name].update(table_meta)

        else:
            self.meta = {
                db_name: table_meta
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
