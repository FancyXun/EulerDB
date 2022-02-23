class Rewriter(object):
    def __init__(self, db_meta):
        self.db_meta = db_meta

    def rewrite(self, *args, **kwargs):
        raise NotImplementedError

    @staticmethod
    def split_table_col(select_val, table):
        select_val_list = select_val.split(".")
        assert select_val_list[0] in table
        return select_val_list[0], select_val_list[1]