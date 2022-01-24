from backend.executors import RemoteExecutor
from backend.executors import DecryptQueryExecutor
from ply.sql_metadata.parser import Parser
from handers.decrypt_handler import DecryptHandler


def invoke(conn, query):
    """

    """
    # todo: get sql command from query
    executor = RemoteExecutor(conn)
    executor.call(query, Parser(query))
    return DecryptQueryExecutor(DecryptHandler).decrypt(executor.result)

