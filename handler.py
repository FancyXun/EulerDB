from abc import ABC
import abc
import json
import logging
import sqlite3
import yaml

import tornado.web
from tornado import gen
from tornado.web import HTTPError
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor
import mysql.connector

from controller.rewriter import ControllerDatabase, ControllerRewriter, \
    ControllerEncryptSql, ControllerDatabase_jar, ControllerEncryptSql1, ControllerEncryptSql2, ControllerDecrypt

NUMBER_OF_EXECUTOR = 100

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

with open("config.yaml", 'r', encoding='utf-8') as f:
    cfg = f.read()
    config = yaml.full_load(cfg)
    if config['meta']['type'] == 'mysql':
        db_conn = mysql.connector.connect(
            host=config['meta']['mysql']["host"],
            database=config['meta']['mysql']["db"],
            user=config['meta']['mysql']["user"],
            passwd=config['meta']['mysql']["passwd"],
            port=int(config['meta']['mysql']["port"])
        )
    else:
        db_conn = sqlite3.connect(config['meta']['sqlite'])


def test_connection(_db_conn):
    try:
        _db_conn.ping()
    except:
        _db_conn = mysql.connector.connect(
            host=config['meta']['mysql']["host"],
            database=config['meta']['mysql']["db"],
            user=config['meta']['mysql']["user"],
            passwd=config['meta']['mysql']["passwd"],
            port=int(config['meta']['mysql']["port"])
        )
    return _db_conn


class BasePostRequestHandler(tornado.web.RequestHandler):

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)

    @gen.coroutine
    def post(self, *args, **kwargs):
        try:
            result = yield self._post(*args, **kwargs)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            logger.error(e)
            raise HTTPError(404, "No results")

    @run_on_executor
    def _post(self, *args, **kwargs):
        request = self._post_request_arguments(*args, **kwargs)
        res = self._request_service(**request)
        return res

    @abc.abstractmethod
    def _post_request_arguments(self, *args, **kwargs):
        raise NotImplementedError('call to abstract method %s._get_request_arguments' % self.__class__)

    @abc.abstractmethod
    def _request_service(self, **kwargs):
        raise NotImplementedError('call to abstract method %s._request_service' % self.__class__)


class PostHandler(BasePostRequestHandler, ABC):

    def _post_request_arguments(self, *args, **kwargs):

        """
        :param args:
        :param kwargs:
        :return:
        """

        logger.info(self.__class__.__name__)
        data = json.loads(self.request.body)
        if not data:
            raise HTTPError(400, "Query argument cannot be empty string")
        return data

    def _request_service(self, **kwargs):

        """
        :param kwargs:
        :return:
        """

        if kwargs:
            c_e = ControllerDatabase(kwargs)
            res = c_e.do_query()
        else:
            raise HTTPError(400, "Query argument cannot be empty string")
        return res


class PostHandler_jar(BasePostRequestHandler, ABC):

    def _post_request_arguments(self, *args, **kwargs):

        """
        :param args:
        :param kwargs:
        :return:
        """

        logger.info(self.__class__.__name__)
        data = {}
        if isinstance(self.request.arguments, dict):
            for k, v in self.request.arguments.items():
                data[k] = v[0].decode('UTF-8')
        if not data:
            raise HTTPError(400, "Query argument cannot be empty string")
        return data

    def _request_service(self, **kwargs):

        """
        :param kwargs:
        :return:
        """

        if kwargs:
            c_e = ControllerDatabase_jar(kwargs)
            res = c_e.do_query()
        else:
            raise HTTPError(400, "Query argument cannot be empty string")
        return res


class RewriteHandler(BasePostRequestHandler, ABC):

    def _post_request_arguments(self, *args, **kwargs):

        """
        :param args:
        :param kwargs:
        :return:
        """

        logger.info(self.__class__.__name__)
        try:
            data = json.loads(self.request.body)
        except Exception as e:
            data = self.request.arguments
        if not data:
            raise HTTPError(400, "Query argument cannot be empty string")
        return data

    def _request_service(self, **kwargs):

        """
        :param kwargs:
        :return:
        """

        if kwargs:
            c_e = ControllerRewriter(kwargs)
            res = c_e.do_rewrite()
        else:
            raise HTTPError(400, "Query argument cannot be empty string")
        return res


class QueryHandler(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)
    ENCRYPT_KEY = "1234salt1234salt"
    ENCRYPT_IV = "5678salt5678salt"

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    def options(self):
        self.set_status(204)
        self.finish()

    @run_on_executor
    def _post(self, *args, **kwargs):
        query_para = json.loads(self.request.body)
        encrypted_columns = {}
        ciphertext = True
        if "create_table" in query_para.keys():
            query = query_para['create_table']
            data_source_id = query_para['selectedJdbcDataSource']['value']
            encrypted_columns = query_para['encrypted_columns']
        else:
            query = query_para['sqlQuery']
            while query[-1] == ";":
                query = query[:-1]
            data_source_id = query_para["jdbcDataSourceId"]
            ciphertext = query_para['ciphertext']
        query = query
        cx = test_connection(db_conn)
        cu = cx.cursor()
        cu.execute("SELECT id, name, connection_url, driver_class_name, "
                   "username, password, ping FROM p_datasource WHERE id={}".format(data_source_id))
        db_info = cu.fetchall()
        cx.commit()
        cu.close()
        jdbc = db_info[0][2]
        if jdbc[:13] == "jdbc:mysql://":
            host_port = jdbc[13:].split("/")[0]
            host = host_port.split(":")[0]
            port = host_port.split(":")[1]
            db = jdbc[13:].split("/")[1]
            user = db_info[0][4]
            password = db_info[0][5]
            kwargs = {
                'host': host,
                'db': db,
                'user': user,
                'password': password,
                'query': query,
                'port': int(port),
                'encrypted_columns': encrypted_columns,
                'ciphertext': ciphertext
            }
            if 'resultLimit' in query_para.keys():
                kwargs['limit'] = int(query_para['resultLimit'])
            c_e = ControllerDatabase(kwargs)
            if "create_table" not in query_para.keys():
                res = c_e.do_query()
                return self.format_result(res)
            else:
                c_e.do_create()
                return {}
        else:
            return {}

    @gen.coroutine
    def post(self, *args, **kwargs):
        try:
            result = yield self._post(*args, **kwargs)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            self.write({'error': str(e)})
            logger.error(e)
            raise HTTPError(404, "No results")

    @staticmethod
    def format_result(res):
        if not res['result']:
            return {}
        columns = res['columns']
        format_res = {'columns': [],
                      "data": [],
                      "error": None}
        order_columns = []
        for k, v in zip(*columns):
            format_res['columns'].append(build_columns(k, v))
            order_columns.append(k)
        result = res["result"]
        data = []
        if result:
            if len(result[0]) > len(order_columns):
                order_columns = ["null" + str(i) for i in range(len(result[0]))]
                format_res['columns'] = []
                for k, v in zip(order_columns, result[0]):
                    tmp = build_columns(k, 'int') if isinstance(v, int) else build_columns(k, 'varchar')
                    format_res['columns'].append(tmp)
        for row in result:
            format_row = {}
            for k, v in zip(order_columns, row):
                format_row[k] = v
            data.append(json.dumps(format_row))
        format_res["data"] = "[" + ",".join(data) + "]"
        return format_res


class EncryptSqlHandler(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)
    ENCRYPT_KEY = "1234salt1234salt"
    ENCRYPT_IV = "5678salt5678salt"

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    def options(self):
        self.set_status(204)
        self.finish()

    @run_on_executor
    def _post(self, *args, **kwargs):
        query_para = json.loads(self.request.body)
        if 'backend' in query_para.keys():
            c_e = ControllerEncryptSql(query_para)
            res = c_e.do_convert()
            return res
        query = query_para['sqlQuery']
        while query[-1] == ";":
            query = query[:-1]
        data_source_id = query_para["jdbcDataSourceId"]
        query = query
        cx = test_connection(db_conn)
        cu = cx.cursor()
        cu.execute("SELECT id, name, connection_url, driver_class_name, "
                   "username, password, ping FROM p_datasource WHERE id={}".format(data_source_id))
        db_info = cu.fetchall()
        cx.commit()
        cu.close()
        jdbc = db_info[0][2]
        if jdbc[:13] == "jdbc:mysql://":
            host_port = jdbc[13:].split("/")[0]
            host = host_port.split(":")[0]
            port = host_port.split(":")[1]
            db = jdbc[13:].split("/")[1]
            user = db_info[0][4]
            password = db_info[0][5]
            kwargs = {
                'host': host,
                'db': db,
                'user': user,
                'password': password,
                'query': query,
                'port': int(port),
            }
            c_e = ControllerEncryptSql(kwargs)
            res = c_e.do_convert()
            return res
        else:
            return {'encrypt_sql': ''}

    @gen.coroutine
    def post(self, *args, **kwargs):
        try:
            result = yield self._post(*args, **kwargs)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            self.write({'error': str(e)})
            logger.error(e)
            raise HTTPError(404, "No results")


class EncryptSqlHandler1(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)
    ENCRYPT_KEY = "1234salt1234salt"
    ENCRYPT_IV = "5678salt5678salt"

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    def options(self):
        self.set_status(204)
        self.finish()

    @run_on_executor
    def _post(self, *args, **kwargs):
        para = json.loads(self.request.body)
        kwargs = {
            'db': para['db'],
            'sql': para['sql'],
        }

        c_e = ControllerEncryptSql1(kwargs)
        res = c_e.do_convert()
        return res


    @gen.coroutine
    def post(self, *args, **kwargs):
        try:
            result = yield self._post(*args, **kwargs)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            self.write({'error': str(e)})
            logger.error(e)
            raise HTTPError(404, "No results")


class EncryptSqlHandler2(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)
    ENCRYPT_KEY = "1234salt1234salt"
    ENCRYPT_IV = "5678salt5678salt"

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    def options(self):
        self.set_status(204)
        self.finish()

    @run_on_executor
    def _post(self, *args, **kwargs):
        para = json.loads(self.request.body)
        kwargs = {
            'db': para['db'],
            'sql': para['sql'],
        }

        c_e = ControllerEncryptSql2(kwargs)
        res = c_e.do_convert()
        return res


    @gen.coroutine
    def post(self, *args, **kwargs):
        try:
            result = yield self._post(*args, **kwargs)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            self.write({'error': str(e)})
            logger.error(e)
            raise HTTPError(404, "No results")


class DecryptResult(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)
    ENCRYPT_KEY = "1234salt1234salt"
    ENCRYPT_IV = "5678salt5678salt"

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    def options(self):
        self.set_status(204)
        self.finish()

    @run_on_executor
    def _post(self, *args, **kwargs):
        """
        columns:col1,col2,col3,col4, rows:r1c1,r1c2,r1c3,r1c4;r2c1,r2c2,r2c3,r2c4

        """
        para = json.loads(self.request.body)
        # print(f"req data:{para}")
        kwargs = {
            'query_id': para['query_id'],
            'data': para['data'],
        }

        c_e = ControllerDecrypt(kwargs)
        res = c_e.do_convert()
        return res


    @gen.coroutine
    def post(self, *args, **kwargs):
        try:
            result = yield self._post(*args, **kwargs)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            self.write({'error': str(e)})
            logger.error(e)
            raise HTTPError(404, "No results")


class CreateHandler(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)
    ENCRYPT_KEY = "1234salt1234salt"
    ENCRYPT_IV = "5678salt5678salt"

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    def options(self):
        self.set_status(204)
        self.finish()
        self.write('{"errorCode":"00","errorMessage","success"}')

    @run_on_executor
    def _post(self, *args, **kwargs):
        query_para = json.loads(self.request.body)
        data_source_id = query_para['selectedJdbcDataSource']['value']
        query, encrypted_columns = self.generate_table(query_para['dataSource'], query_para['count'],
                                                       query_para['table_name'])
        cx = test_connection(db_conn)
        cu = cx.cursor()
        cu.execute("SELECT id, name, connection_url, driver_class_name, "
                   "username, password, ping FROM p_datasource WHERE id={}".format(data_source_id))
        db_info = cu.fetchall()
        cx.commit()
        cu.close()
        jdbc = db_info[0][2]
        if jdbc[:13] == "jdbc:mysql://":
            host_port = jdbc[13:].split("/")[0]
            host = host_port.split(":")[0]
            port = host_port.split(":")[1]
            db = jdbc[13:].split("/")[1]
            user = db_info[0][4]
            password = db_info[0][5]
            kwargs = {
                'host': host,
                'db': db,
                'user': user,
                'password': password,
                'query': query,
                'port': int(port),
                'encrypted_columns': encrypted_columns
            }
            if 'resultLimit' in query_para.keys():
                kwargs['limit'] = int(query_para['resultLimit'])
            c_e = ControllerDatabase(kwargs)
            c_e.do_create()
            return {}
        else:
            return {}

    @gen.coroutine
    def post(self, *args, **kwargs):
        try:
            result = yield self._post(*args, **kwargs)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            logger.error(e)
            raise HTTPError(404, "No results")

    @staticmethod
    def generate_table(columns, size, table_name):
        query = "create table if not exists {}".format(table_name)
        encrypted_columns = {}
        part = []
        for i in range(size):
            col = columns[i]
            if int(col['length']) > 0 and col['type'].lower() not in ['int']:
                part.append(col['name'] + " " + col['type'] + "(" + col['length'] + ")")
            else:
                part.append(col['name'] + " " + col['type'])
            if col['encryption'].lower() == 'true':
                if col['fuzzy'].lower() == 'true':
                    encrypted_columns[col['name']] = {
                        "fuzzy": True,
                        "key": col['token']
                    }
                else:
                    encrypted_columns[col['name']] = {
                        "fuzzy": False,
                        "key": col['token']
                    }
        part = ", ".join(part)
        query = query + "(" + part + ")"
        return query, encrypted_columns


def build_columns(k, v):
    if v == 'varchar':
        return {"name": k,
                "javaType": "VARCHAR",
                "dbType": "VARCHAR",
                "length": 100
                }
    else:
        return {"name": k,
                "javaType": "INTEGER",
                "dbType": "INT",
                "length": 10
                }


class QueryComponentHandler(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)
    ENCRYPT_KEY = "1234salt1234salt"
    ENCRYPT_IV = "5678salt5678salt"

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    def options(self, component_id=None):
        self.set_status(204)
        self.finish()
        self.write('{"errorCode":"00","errorMessage","success"}')

    @run_on_executor
    def _post(self, component_id=None):
        query_para = json.loads(self.request.body)
        cx = test_connection(db_conn)
        cu = cx.cursor()
        cu.execute("SELECT id, name, connection_url, driver_class_name, "
                   "username, password, ping FROM p_datasource WHERE id={}".format(component_id))
        db_info = cu.fetchall()
        cx.commit()
        cu.close()
        jdbc = db_info[0][2]
        if jdbc[:13] == "jdbc:mysql://":
            host_port = jdbc[13:].split("/")[0]
            host = host_port.split(":")[0]
            port = host_port.split(":")[1]
            db = jdbc[13:].split("/")[1]
            user = db_info[0][4]
            password = db_info[0][5]
            kwargs = {
                'host': host,
                'db': db,
                'user': user,
                'password': password,
                'port': int(port),
            }
            c_e = ControllerDatabase(kwargs)
            if "create_table" not in query_para.keys():
                res = c_e.do_query()
                return self.format_result(res)
            else:
                c_e.do_create()
        else:
            return {}

    @gen.coroutine
    def post(self, component_id=None):
        try:
            result = yield self._post(component_id)
            self.write(result)
        except HTTPError as e:
            self.write(e)
        except Exception as e:
            logger.error(e)
            raise HTTPError(404, "No results")

    @staticmethod
    def format_result(res):
        if not res['result']:
            return {}
        columns = res['columns']
        format_res = {'columns': [],
                      "data": [],
                      "error": None}
        order_columns = []
        for k, v in columns.items():
            if v == 'varchar':
                format_res['columns'].append({"name": k,
                                              "javaType": "VARCHAR",
                                              "dbType": "VARCHAR",
                                              "length": 100
                                              })
            elif v == 'int':
                format_res['columns'].append({"name": k,
                                              "javaType": "INTEGER",
                                              "dbType": "INT",
                                              "length": 10
                                              })
            order_columns.append(k)
        result = res["result"]
        data = []
        for row in result:
            format_row = {}
            for k, v in zip(order_columns, row):
                format_row[k] = v
            data.append(json.dumps(format_row))
        format_res["data"] = "[" + ",".join(data) + "]"
        return format_res


class SchemaHandler(tornado.web.RequestHandler, ABC):
    executor = ThreadPoolExecutor(NUMBER_OF_EXECUTOR)

    def set_default_headers(self):
        self.set_header('Access-Control-Allow-Origin', '*')
        self.set_header('Access-Control-Allow-Headers', '*')
        self.set_header('Access-Control-Max-Age', 1000)
        self.set_header('Content-type', 'application/json')
        self.set_header('Access-Control-Allow-Methods', 'POST, GET, OPTIONS')
        self.set_header('Access-Control-Allow-Headers',
                        'Content-Type, Access-Control-Allow-Origin, '
                        'Access-Control-Allow-Headers, X-Requested-By, Access-Control-Allow-Methods')

    @run_on_executor
    def get(self, component_id=None):
        cx = test_connection(db_conn)
        cu = cx.cursor()
        cu.execute("SELECT id, name, connection_url, driver_class_name, "
                   "username, password, ping FROM p_datasource WHERE id={}".format(component_id))
        db_info = cu.fetchall()
        connection_url = db_info[0][2]
        if connection_url[:13] == "jdbc:mysql://":
            db = connection_url[13:]
            cu.execute(
                "SELECT table_name, col_info FROM p_db_meta WHERE database_name='{}' order by table_name".format(db))
            self.write(self.format_result(cu.fetchall()))
        else:
            self.write({})
        cx.commit()
        cu.close()

    @staticmethod
    def format_result(tables_info):
        res = []
        table = {}
        for row in tables_info:
            format_col = {}
            if not table:
                table["name"] = row[0]
                table["type"] = "USER TABLE"
                table["columns"] = []
            col = json.loads(row[1])
            for key, value in col.items():
                if value['type'] == 'varchar':
                    format_col = {
                        "name": key if value['plaintext'] else key + " *",
                        "javaType": "VARCHAR",
                        "dbType": "VARCHAR",
                        "length": 258
                    }
                elif value['type'] == 'int':
                    format_col = {
                        "name": key if value['plaintext'] else key + " *",
                        "javaType": "INTEGER",
                        "dbType": "INT UNSIGNED",
                        "length": 20
                    }
            if row[0] == table["name"]:
                table["columns"].append(format_col)
            else:
                res.append(json.dumps(table))
                table = {"name": row[0], "type": "USER TABLE", "columns": []}
                table["columns"].append(format_col)
        res.append(json.dumps(table))
        return "[" + ",".join(res) + "]"
