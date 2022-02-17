from scheduler.backend import execution_context


class ControllerDatabase(object):

    def __init__(self, data):
        self.query_info = data

    def do_query(self):
        encrypted_cols = None
        if "encrypted_columns" in self.query_info.keys():
            encrypted_cols = self.query_info['encrypted_columns']
        result = execution_context.invoke(self.query_info, self.query_info['query'], encrypted_cols, columns_info=True)
        if result[0]:
            return {'result': result[0],
                    'columns': result[1]}
        else:
            return {'result': []}

    def do_create(self):
        query = self.query_info['query']
        encrypted_cols = self.query_info['encrypted_columns']
        execution_context.invoke(self.query_info, query, encrypted_cols, columns_info=True)


class ControllerRewriter(object):

    def __init__(self, data):
        self.query_info = data

    def do_rewrite(self):
        query = self.query_info['query']
        if isinstance(query, list):
            query = query[0].decode("utf-8")
        db = self.query_info['db']
        if isinstance(db, list):
            db = db[0].decode("utf-8")
        encrypted_cols = None
        if "encrypt_cols" in self.query_info.keys():
            encrypted_cols = self.query_info['encrypt_cols']
        return execution_context.rewrite(query, db, encrypted_cols)
