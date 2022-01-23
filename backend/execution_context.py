from backend.executors import RemoteExecutor
from backend.executors import DecryptQueryExecutor
from ply.ply import lex
from ply.ply import yacc


def invoke(conn, query):
    """

    """
    # todo: get sql command from query
    parser = yacc.yacc()
    parser.parse(query)
    result = RemoteExecutor(conn).call(query)
    return DecryptQueryExecutor().decrypt(result)

