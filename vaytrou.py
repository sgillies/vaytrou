import os

from repoze.zodbconn.finder import PersistentApplicationFinder
from rtree import Rtree
import tornado.httpserver
import tornado.ioloop
import tornado.web

from vrtree import VRtreeIndex

def appmaker(root):
    if not 'index' in root:
        index = VRtreeIndex()
        root['index'] = index
        import transaction
        transaction.commit()
    return root['index']

index = None

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("index has %s items\n" % len(index.bwd))

application = tornado.web.Application([
    (r"/", MainHandler),
])

if __name__ == "__main__":
    finder = PersistentApplicationFinder('file://%s/Data.fs' % os.path.dirname(os.path.abspath(__file__)), appmaker)
    environ = {}
    index = finder(environ)
    index.fwd = Rtree('v1')
    http_server = tornado.httpserver.HTTPServer(application)
    http_server.listen(8888)
    try:
        loop = tornado.ioloop.IOLoop.instance()
        loop.start()
    except KeyboardInterrupt:
        loop.stop()
        print "Exiting."
    finally:
        index.fwd.close()

