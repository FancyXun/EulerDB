from scheduler.backend import execution_context
import yaml

with open("config.yaml", 'r', encoding='utf-8') as f:
    cfg = f.read()
    config = yaml.full_load(cfg)
    host = config['back_db'][config['back_db']['type']]["host"]
    port = config['back_db'][config['back_db']['type']]["port"]


class ControllerDatabase(object):

    def __init__(self, data):
        self.query_info = data

    def do_query(self):
        encrypted_cols = None
        if "encrypted_columns" in self.query_info.keys():
            encrypted_cols = self.query_info['encrypted_columns']
        if 'batch_process' in self.query_info.keys():
            execution_context.batch_process(self.query_info)
            return {'result': []}
        result = execution_context.invoke(self.query_info, self.query_info['query'], encrypted_cols, columns_info=True)
        if result[0]:
            return {'result': result[0],
                    'columns': result[1]}
        else:
            return {'result': []}

    def do_create(self):
        query = self.query_info['query']
        encrypted_cols = self.query_info['encrypted_columns']
        encrypted_cols = eval(encrypted_cols) if isinstance(encrypted_cols, str) else encrypted_cols
        execution_context.invoke(self.query_info, query, encrypted_cols, columns_info=True)


class ControllerDatabase_jar(object):

    def __init__(self, data):
        self.query_info = data

    def do_query(self):
        self.query_info['host'] = host
        self.query_info['port'] = port
        result = execution_context.invoke(self.query_info, self.query_info['query'], None, columns_info=True)
        if result[0]:
            return {'result': result[0],
                    'columns': result[1]}
        else:
            return {'result': []}


class ControllerEncryptSql(object):

    def __init__(self, data):
        self.query_info = data

    def do_convert(self):
        result = execution_context.encrypt_sql(self.query_info, self.query_info['query'])
        if result:
            return {'encrypt_sql': result}
        else:
            return {'encrypt_sql': ''}


class ControllerEncryptSql1(object):

    def __init__(self, data):
        self.data = data

    def do_convert(self):
        if self.data['sql'] == "select @@version_comment limit 1":
            return {'encrypt_sql': 'select @@version_comment limit 1'}
        try:
            result = execution_context.encrypt_sql1(self.data['db'], self.data['sql'])
        except Exception as e:
            result = self.data['sql']
        print(self.data['sql'], result)
        if result:
            return {'encrypt_sql': result}
        else:
            return {'encrypt_sql': ''}


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
