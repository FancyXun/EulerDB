import json
import yaml
import sqlite3
import mysql.connector


with open("config.yaml", 'r', encoding='utf-8') as f:
    cfg = f.read()
    config = yaml.full_load(cfg)
    if config['meta']['type'] == 'mysql':
        cx = mysql.connector.connect(
              host=config['meta']['mysql']["host"],
              database=config['meta']['mysql']["db"],
              user=config['meta']['mysql']["user"],
              passwd=config['meta']['mysql']["passwd"],
              port=int(config['meta']['mysql']["port"])
            )
        insert_sql = 'insert into p_db_meta values(%s,%s,%s,%s,%s,%s)'
    else:
        cx = sqlite3.connect(config['meta']['sqlite'], check_same_thread=False)
        insert_sql = 'insert into p_db_meta values (?,?,?,?,?,?)'

element_type = {
    "INT":
        {
            "order-preserving": "BIGINT",
            "symmetric": "VARCHAR(300)"
        },
    "VARCHAR":
        {
            "symmetric": 20
        }
}

fuzzy_type_fixed = 'VARCHAR(2000)'

FUNC_CIPHERS = {
    "max": "order-preserving",
    "min": "order-preserving"
}


class Delta(object):
    __instance = None
    meta = None
    cx = None

    def __new__(cls, *args, **kwargs):
        if Delta.__instance is None:
            Delta.__instance = object.__new__(cls, *args, **kwargs)
            cls.cx = cx
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
        # update database meta
        self.update_db_meta(db_name, table_meta)
        return self.meta

    def delete_delta(self, db, table):
        self.meta[db].pop(table, None)
        cu = self.cx.cursor()
        sql = "delete from p_db_meta where database_name = '{}' and table_name = '{}'".format(db, table)
        cu.execute(sql)
        if config['meta']['type'] == 'mysql':
            self.cx.commit()
        else:
            cu.connection.commit()

    def update_db_meta(self, db_name, table_meta):
        cu = self.cx.cursor()
        rows = []
        for t_name, t_value in table_meta.items():
            table_name = t_name
            table_anonymous = t_value['anonymous']
            for key, value in t_value['columns'].items():
                col_str = json.dumps({key: value})
                rows.append((db_name, 'mysql', table_name, table_anonymous, col_str, ''))
        cu.executemany(insert_sql, rows)
        if config['meta']['type'] == 'mysql':
            self.cx.commit()
        else:
            cu.connection.commit()

    @classmethod
    def load_delta(cls):
        meta = {}
        cu = cls.cx.cursor()
        cu.execute("select * from p_db_meta")
        db_meta = cu.fetchall()
        if not db_meta:
            return meta
        for row in db_meta:
            db_name, table_name, table_anonymous, col_info = row[0], row[2], row[3], row[4]
            if db_name not in meta.keys():
                meta[db_name] = {}
            if table_name not in meta[db_name].keys():
                meta[db_name][table_name] = {}
            if 'anonymous' not in meta[db_name][table_name].keys():
                meta[db_name][table_name]['anonymous'] = table_anonymous
            if 'columns' not in meta[db_name][table_name].keys():
                meta[db_name][table_name]['columns'] = {}
            meta[db_name][table_name]['columns'].update(json.loads(col_info))
        return meta
