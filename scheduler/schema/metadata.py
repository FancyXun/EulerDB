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
    "VARCHAR": encrypt.FernetCipher(),
    "INT": [encrypt.OPECipher(), encrypt.FernetCipher()]
}

CIPHERS_META ={
    "OPE": encrypt.OPECipher(),
    "SYMMETRIC": encrypt.FernetCipher()
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
            if db_name not in self.meta["plain"].keys():
                # update database
                self.meta["plain"].update({db_name: {table_name["origin"]: table_meta}})
                self.meta["cipher"].update({db_name: {table_name["anonymous"]: anonymous_meta}})
                self.meta["table_kv"].update({db_name: {table_name["origin"]: table_name["anonymous"]}})
            else:
                self.meta["plain"][db_name].update({table_name["origin"]: table_meta})
                self.meta["cipher"][db_name].update({table_name["anonymous"]: anonymous_meta})
                self.meta["table_kv"][db_name].update({table_name["origin"]: table_name["anonymous"]})

        else:
            self.meta = {"plain": {db_name: {table_name["origin"]: table_meta}},
                         "cipher": {db_name: {table_name["anonymous"]: anonymous_meta}},
                         "table_kv": {db_name: {table_name["origin"]: table_name["anonymous"]}}
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
