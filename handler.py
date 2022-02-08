from abc import ABC

from controller.rewriter import ControllerDatabase

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
