from abc import ABC

from controller.rewriter import ControllerDatabase, ControllerRewriter

import abc
import json
import logging

import tornado.web
from tornado import gen
from tornado.web import HTTPError
from tornado.concurrent import run_on_executor
from concurrent.futures import ThreadPoolExecutor

NUMBER_OF_EXECUTOR = 6

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


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
        query = query_para['sqlQuery']
        while query[-1] == ";":
            query = query[:-1]
        kwargs = {
            'host': '127.0.0.1',
            'db': 'points',
            'user': 'root',
            'password': 'root',
            'query': query
        }
        c_e = ControllerDatabase(kwargs)
        res = c_e.do_query()
        return self.format_result(res)

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
