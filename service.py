import logging

import tornado.web
from tornado.web import URLSpec
from tornado.ioloop import IOLoop


from handler import PostHandler, RewriteHandler, QueryHandler, QueryComponentHandler, \
    SchemaHandler, CreateHandler, EncryptSqlHandler, PostHandler_jar, EncryptSqlHandler1, EncryptSqlHandler2, DecryptResult

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


HANDLERS = [
    URLSpec(r'/query', PostHandler,
            name=PostHandler.__name__),
    URLSpec(r'/query_jar', PostHandler_jar,
            name=PostHandler_jar.__name__),
    URLSpec(r'/rewrite_query', RewriteHandler,
            name=RewriteHandler.__name__),
    URLSpec(r'/jdbc_query', QueryHandler,
            name=QueryHandler.__name__),
    URLSpec(r'/create_table', CreateHandler,
            name=CreateHandler.__name__),
    URLSpec(r'/jdbc_query/component/?([?P<component_id>\w])?', QueryComponentHandler,
            name=QueryComponentHandler.__name__),
    URLSpec(r'/jdbc_query/schema/?([?P<component_id>\w])?', SchemaHandler,
            name=SchemaHandler.__name__),
    URLSpec(r'/encrypt_sql', EncryptSqlHandler,
            name=SchemaHandler.__name__),
    URLSpec(r'/encrypt_sql1', EncryptSqlHandler1,
            name=SchemaHandler.__name__),
    URLSpec(r'/encrypt_sql2', EncryptSqlHandler2,
            name=SchemaHandler.__name__),
    URLSpec(r'/decryptd_data', DecryptResult,
                name=SchemaHandler.__name__),
]

if __name__ == '__main__':
    SERVER_PORT = 8889
    app = tornado.web.Application(handlers=HANDLERS, debug=True)
    app.listen(SERVER_PORT)
    logger.info("Clean Panel server started on port {SERVER_PORT}".format(SERVER_PORT=SERVER_PORT))
    IOLoop.current().start()