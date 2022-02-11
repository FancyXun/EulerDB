from scheduler.backend.executors import RemoteExecutor
from scheduler.backend.executors import DecryptQueryExecutor
from scheduler.compile.parser import Parser
from scheduler.handers.decrypt_handler import DecryptHandler


def invoke(conn, query, encrypted_cols=None):
    """
    At present, we temporarily use parser class to determine SQL type.
    But in fact
    this is a not good choice. it would affect system's performance if parsing SQL for many times,
    this is unnecessary work. We would remove the parser class later.

    Remote executor is responsible for connecting with the remote database and send the rewritten SQL,
    in the same time, it would decide if saving some changes to the meta.json or not.

    The decrypt query executor is for decrypting the returned results.
    Of course, the returned results may also include plaintext results, so we need to keep these results
    without doing anything
    """
    executor = RemoteExecutor(conn)
    executor.call(query, Parser(query), encrypted_cols)
    return DecryptQueryExecutor(DecryptHandler).decrypt(executor)


def rewrite(query, db, encrypted_cols=None):
    return RemoteExecutor(None).dispatch(query, db, encrypted_cols)


