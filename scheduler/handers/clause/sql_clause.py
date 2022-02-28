from scheduler.handers.clause import clause
from scheduler.schema.metadata import Delta


class Clause(object):
    def __init__(self, db, encrypted_cols=None):
        super().__init__()
        self.db = db
        self.db_meta = {}
        self.encrypted_cols = encrypted_cols
        self.origin_query = None
        if Delta().meta:
            self.db_meta = Delta().meta.get(db, {})
        for key, value in clause.items():
            if isinstance(value, dict):
                for k, v in value.items():
                    setattr(self, key, {k: v(self.db_meta)})
            else:
                setattr(self, key, value(self.db_meta))


if __name__ == "__main__":
    t = Clause({})

