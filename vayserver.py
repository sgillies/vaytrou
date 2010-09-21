
import tornado.httpserver
import tornado.ioloop
from tornado.options import parse_config_file, parse_command_line

from vaytrou.app import make_app

if __name__ == '__main__':
    parse_config_file('etc/server.conf')
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

