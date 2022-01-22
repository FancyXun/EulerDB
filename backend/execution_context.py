from backend.executors import RemoteExecutor
from backend.executors import DecryptQueryExecutor


def invoke(conn, query):
    """

    """
    # todo: get sql command from query
    result = RemoteExecutor(conn).call(query)
    return DecryptQueryExecutor().decrypt(result)

