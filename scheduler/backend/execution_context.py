from scheduler.backend.executors import RemoteExecutor
from scheduler.backend.executors import DecryptQueryExecutor
from scheduler.compile.parser import Parser
from scheduler.handers.decrypt_handler import DecryptHandler


def invoke(conn, query, encrypted_cols=None):
    """

    """
    executor = RemoteExecutor(conn)
    executor.call(query, Parser(query), encrypted_cols)
    return DecryptQueryExecutor(DecryptHandler).decrypt(executor.result)

