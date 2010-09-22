import os

import tornado.httpserver
import tornado.ioloop
from tornado.options import options, parse_config_file, parse_command_line

from vaytrou.app import make_app

# TODO: import sys and add egg paths

if __name__ == '__main__':
    parse_command_line()
    config_file = options.config or os.path.join(
        os.getcwd(), 'etc', 'vaytrou.conf')
    parse_config_file(config_file)
    environ = {}
    application = make_app(environ)
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    try:
        loop = tornado.ioloop.IOLoop.instance()
        loop.start()
    except KeyboardInterrupt:
        loop.stop()
        print 'Exiting.'
    finally:
        application.index.close()

