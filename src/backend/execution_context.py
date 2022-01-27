from src.backend.executors import RemoteExecutor
from src.backend.executors import DecryptQueryExecutor
from src.lex_module.sql_metadata.parser import Parser
from src.handers.decrypt_handler import DecryptHandler


def invoke(conn, query):
    """

    """
    # todo: get compile command from query
    executor = RemoteExecutor(conn)
    executor.call(query, Parser(query))
    return DecryptQueryExecutor(DecryptHandler).decrypt(executor.result)

