# encoding: utf-8
#
#
# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this file,
# You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Contact: Kyle Lahnakoski (kyle@lahnakoski.com)
#
from __future__ import absolute_import, division, unicode_literals

from collections import namedtuple
from types import FunctionType

from dots import Null
from parsing_future import get_function_arguments, get_function_name, get_function_defaults


class cache(object):
    """
    :param func: ASSUME FIRST PARAMETER OF `func` IS `self`
    :param duration: USE CACHE IF LAST CALL WAS LESS THAN duration AGO
    :param lock: True if you want multithreaded monitor (default False)
    :param ignore: Parameters to ignore while caching
    :return:
    """

    def __new__(cls, *args, **kwargs):
        if len(args) == 1 and isinstance(args[0], FunctionType):
            func = args[0]
            return wrap_function(_SimpleCache(), func)
        else:
            return object.__new__(cls)

    def __call__(self, func):
        return wrap_function(self, func)


class _SimpleCache(object):

    def __init__(self):
        self.timeout = Null
        self.locker = _FakeLock()


def wrap_function(cache_store, func_):
    attr_name = "_cache_for_" + func_.__name__

    func_name = get_function_name(func_)
    params = get_function_arguments(func_)
    if not get_function_defaults(func_):
        defaults = {}
    else:
        defaults = {
            k: v
            for k, v in zip(reversed(params), reversed(get_function_defaults(func_)))
        }
    func_args = get_function_arguments(func_)
    if len(func_args) > 0 and func_args[0] == "self":
        using_self = True
        func = lambda self, *args: func_(self, *args)
    else:
        using_self = False
        func = lambda self, *args: func_(*args)


CacheElement = namedtuple("CacheElement", ("timeout", "key", "value", "exception"))


class _FakeLock():
    def __enter__(self):
        pass

    def __exit__(self, exc_type, exc_val, exc_tb):
        pass
