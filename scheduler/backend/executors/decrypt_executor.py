import logging
import time
from scheduler.backend.executors.abstract_executor import AbstractQueryExecutor
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
handler = logging.FileHandler('euler_db.log')
handler.setLevel(logging.INFO)
logger.addHandler(handler)


class DecryptQueryExecutor(AbstractQueryExecutor):
    def __init__(self, handler):
        super().__init__(handler)

    def decrypt(self, executor, query_info, select_columns, db_meta, table):
        result = executor.result
        start_time = time.time()
        if result:
            if 'ciphertext' in query_info and query_info['ciphertext']:
                logger.info("decrypt:{}".format(time.time() - start_time))
                return result
            result = self.handler(executor).decrypt(result, select_columns, db_meta, table)
            logger.info("decrypt:{}".format(time.time() - start_time))
            return result
        logger.info("decrypt:{}".format(time.time() - start_time))
        return []

    def decrypt1(self, data, select_state, query_info, select_columns, db_meta, table):
        return self.handler([]).decrypt1(data, select_columns, db_meta, table, select_state)