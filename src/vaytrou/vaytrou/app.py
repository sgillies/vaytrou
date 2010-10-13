import math
from itertools import ifilter, imap, islice
from optparse import OptionParser
import os

from repoze.zodbconn.finder import PersistentApplicationFinder
from rtree import Rtree
from shapely.geometry import asShape, Point
from simplejson import dumps, loads
from tornado.options import define, options
from tornado.options import parse_config_file, parse_command_line
import tornado.web

from indexing import BatchError, ChangeSet, ConflictError
from vrtree import VIntRtreeIndex

define('data', default=None, help='Data storage directory')

def appmaker(root):
    if not 'index' in root:
        index = VIntRtreeIndex()
        root['index'] = index
        import transaction
        transaction.commit()
    return root['index']

def box2poly(b):
    return dict(
        type='Polygon', 
        coords=[(
            (b[0], b[1]),
            (b[0], b[3]),
            (b[2], b[3]),
            (b[2], b[1])
            )]
        )

def geom(item):
    try:
        g = item['geometry']
    except KeyError:
        b = item['bbox']
        g = box2poly(b)
    return asShape(g)

class Application(tornado.web.Application):
    def __init__(self, index=None, handlers=None, default_host="",
        transforms=None, wsgi=False, **settings):
        super(Application, self).__init__(
            handlers, default_host, transforms, wsgi, **settings)
        self.index = index

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write('index has %s items\n' % len(self.application.index.bwd))
    def post(self):
        try:
            data = loads(self.request.body)
            changeset = ChangeSet(
                additions=data.get('index'), deletions=data.get('unindex'))
            self.application.index.batch(changeset)
            self.application.index.commit()
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(dict(msg='Batch success')))
        except (BatchError, ConflictError) as error:
            self.set_status(409)
            self.set_header('content-type', 'application/json')
            self.write(dumps(dict(msg=str(error))))
        except:            
            raise

class IntersectionHandler(tornado.web.RequestHandler):
    def get(self):
        '''Perform intersection search'''
        try:
            bbox = self.get_argument('bbox')
            coords = tuple(float(x) for x in bbox.split(','))
            # Paging args
            start = int(self.get_argument('start', '0'))
            count = int(self.get_argument('count', '20'))
            if count > 20: count = 20
            results = list(islice(self.application.index.intersection(coords), start, start+count))
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(dict(bbox=coords, start=start, count=len(results), items=results)))
        except:
            raise

class NearestHandler(tornado.web.RequestHandler):
    def get(self):
        '''Perform intersection search'''
        try:
            bbox = self.get_argument('bbox')
            coords = tuple(float(x) for x in bbox.split(','))
            limit = int(self.get_argument('limit', '1'))
            if limit > 20: limit = 20
            results = list(self.application.index.nearest(coords, limit))
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(dict(bbox=bbox, limit=limit, count=len(results), items=results)))
        except:
            raise

class DistanceHandler(tornado.web.RequestHandler):
    def get(self):
        '''Perform distance search'''
        try:
            x = float(self.get_argument('lon'))
            y = float(self.get_argument('lat'))
            r = float(self.get_argument('radius'))
            strict = bool(self.get_argument('strict', 0))
            start = int(self.get_argument('start', '0'))
            count = int(self.get_argument('count', '20'))
            if count > 20: count = 20
            # Make a box for first pass intersection search
            Re = 6371000.0 # radius of earth in meters, spherical approx
            F = 180.0/(math.pi*Re)
            d = F*r/math.cos(y)
            coords = (x-d, y-d, x+d, y+d)
            candidates = self.application.index.intersection(coords)
            # Now filter geometrically
            region = Point(x, y).buffer(d)
            # Strict mode means no maybes
            if strict:
                def pred(ob):
                    g = geom(ob)
                    return region.contains(g)
            else:
                def pred(ob):
                    g = geom(ob)
                    return region.intersects(g)
            results = list(islice(ifilter(pred, candidates), start, start+count))
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(dict(lon=x, lat=y, radius=r, strict=strict, start=start, count=len(results), items=results)))
        except:
            raise

def make_app(environ, argv1=None):
    parser = OptionParser()
    parser.add_option(
        "-c", "--config", dest="config_file", 
        help="Configuration file", metavar="FILE")
    parser.add_option(
        "-d", "--data", dest="data",
        help="Data directory", metavar="FILE")
    parser.add_option(
        "-l", "--logfile", dest="log_file_prefix",
        help="Log file", metavar="FILE")
    parser.add_option(
        "-L", "--log-level", dest="logging",
        help="Log level", metavar="info|warning|error|none")
    opts, args = parser.parse_args(argv1)
    
    config_file = opts.config_file or os.path.join(
        os.getcwd(), "etc", "vaytrou.conf")
    if config_file is not None:
        parse_config_file(config_file)    
    parse_command_line([])

    # Load command line options over Tornado options
    for k in options:
        v = getattr(opts, k, None)
        if v is not None:
            options[k].set(v)

    finder = PersistentApplicationFinder(
        'file://%s/Data.fs' % options.data, appmaker)
    index = finder(environ)
    index.fwd = Rtree('%s/vrt1' % options.data)
    
    return Application(index, [
        (r'/', MainHandler),
        (r'/intersection', IntersectionHandler),
        (r'/nearest', NearestHandler),
        (r'/distance', DistanceHandler),
        ])



