from src.backend.executors import RemoteExecutor
from src.backend.executors import DecryptQueryExecutor
from src.compile.parser import Parser
from src.handers.decrypt_handler import DecryptHandler


def invoke(conn, query):
    """

    """
    # todo: get compile command from query
    executor = RemoteExecutor(conn)
    executor.call(query, Parser(query))
    return DecryptQueryExecutor(DecryptHandler).decrypt(executor.result)

