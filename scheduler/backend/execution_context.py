from scheduler.backend.executors import RemoteExecutor
from scheduler.backend.executors import DecryptQueryExecutor
from scheduler.compile.parser import Parser
from scheduler.handers.decrypt_handler import DecryptHandler


def invoke(conn, query):
    """

    """
    # todo: get compile command from query
    executor = RemoteExecutor(conn)
    executor.call(query, Parser(query))
    return DecryptQueryExecutor(DecryptHandler).decrypt(executor.result)

