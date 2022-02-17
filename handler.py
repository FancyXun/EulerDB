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

from controller.rewriter import ControllerDatabase, ControllerRewriter

NUMBER_OF_EXECUTOR = 6

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

with open("config.yaml", 'r', encoding='utf-8') as f:
    cfg = f.read()
    config = yaml.full_load(cfg)


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
        self.write('{"errorCode":"00","errorMessage","success"}')

    @run_on_executor
    def _post(self, *args, **kwargs):
        query_para = json.loads(self.request.body)
        encrypted_columns = {}
        if "create_table" in query_para.keys():
            query = query_para['create_table']
            data_source_id = query_para['selectedJdbcDataSource']['value']
            encrypted_columns = query_para['encrypted_columns']
        else:
            query = query_para['sqlQuery']
            while query[-1] == ";":
                query = query[:-1]
            data_source_id = query_para["jdbcDataSourceId"]
        cx = sqlite3.connect(config['meta']['db'])
        cu = cx.cursor()
        cu.execute("SELECT id, name, connection_url, driver_class_name, "
                   "username, password, ping FROM p_datasource WHERE id={}".format(data_source_id))
        db_info = cu.fetchall()
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
                'encrypted_columns': encrypted_columns
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
