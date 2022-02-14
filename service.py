import logging
import yaml

import tornado.web
from tornado.web import URLSpec
from tornado.ioloop import IOLoop


from handler import PostHandler
from handler import RewriteHandler
from handler import QueryHandler

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


HANDLERS = [
    URLSpec(r'/query', PostHandler,
            name=PostHandler.__name__),
    URLSpec(r'/rewrite_query', RewriteHandler,
            name=RewriteHandler.__name__),
    URLSpec(r'/vue_query', QueryHandler,
            name=QueryHandler.__name__)
]

if __name__ == '__main__':
    SERVER_PORT = 8888
    app = tornado.web.Application(handlers=HANDLERS, debug=True)
    app.listen(SERVER_PORT)
    logger.info("Clean Panel server started on port {SERVER_PORT}".format(SERVER_PORT=SERVER_PORT))
    IOLoop.current().start()