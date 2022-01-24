from crypto import encrypt


class Delta(object):

    __instance = None

    def __init__(self):
        self.meta = {}
        self.keys = {"varchar": encrypt.FernetCipher(),
                     "int": encrypt.FernetCipher()}

    def __new__(cls, *args, **kwargs):
        if Delta.__instance is None:
            Delta.__instance = object.__new__(cls, *args, **kwargs)
        return Delta.__instance

    def update_delta(self, db_name, table_name, table_meta):

        if self.meta:
            pass
        else:
            self.meta = {
                db_name: {table_name: table_meta}
            }

        return self.meta

    def delete_delta(self):
        pass

