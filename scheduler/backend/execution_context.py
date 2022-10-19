from scheduler.backend.executors import RemoteExecutor
from scheduler.backend.executors import ConvertExecutor
from scheduler.backend.executors import DecryptQueryExecutor
from scheduler.compile.parser import Parser
from scheduler.handers.decrypt_handler import DecryptHandler


def invoke(query_info, query_sql, encrypted_cols=None, columns_info=False):
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
    executor = RemoteExecutor(query_info)
    executor.call(query_sql, Parser(query_sql), encrypted_cols)
    select_columns = executor.get_select_columns()
    select_types = executor.get_select_types()
    db_meta, table = executor.get_db_meta()
    if not columns_info:
        return DecryptQueryExecutor(DecryptHandler).decrypt(executor, query_info, select_columns, db_meta, table)

    return [DecryptQueryExecutor(DecryptHandler).decrypt(
        executor, query_info, select_columns, db_meta, table), [select_columns, select_types]]


def encrypt_sql(query_info, query_sql):
    executor = RemoteExecutor(query_info)
    return executor.encrypt_sql(query_sql)


def encrypt_sql1(db, sql):
    executor = ConvertExecutor()
    return executor.dispatch(db, sql)


def batch_process(query_info):
    executor = RemoteExecutor(query_info)
    executor.batch_insert(query_info['batch_process'])


def rewrite(query, db, encrypted_cols=None):
    return RemoteExecutor(None).dispatch(query, db, encrypted_cols)


