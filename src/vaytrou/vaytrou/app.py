import math
from itertools import ifilter, imap, islice
from optparse import OptionParser
import os

from repoze.zodbconn.finder import PersistentApplicationFinder
from rtree import Rtree
from shapely.geometry import asShape, Point
from shapely import wkt
from simplejson import dumps, loads
from tornado.options import define, options
from tornado.options import parse_config_file, parse_command_line
import tornado.web

from indexing import BatchError, ChangeSet, ConflictError
from vrtree import VIntRtreeIndex

define('data', default=None, help='Data storage directory')
define('port', default=8888, help='Server port')
define('page_size', type=int, default=20, help='Search result page size')
define('max_page_size', type=int, default=100, help='Maximum search result page size')
define('max_nearest_limit', type=int, default=20, help='Maximum limit for nearest neighbor search')
define('max_bbox_area', type=float, default=None, help='Maximum search box area')
define('max_radius', type=float, default=200000.0, help='Maximum radius for distance search')

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
        coordinates=[(
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

class Error(Exception):
    pass

class SearchBoundsTooLarge(Error):
    def __init__(self, bbox, value):
        self.bbox = bbox
        self.value = value
    def __str__(self):
        return "Area of %s exceeds %s" % (self.bbox, self.value)

class SearchRadiusTooLarge(Error):
    def __init__(self, radius, value):
        self.radius = radius
        self.value = value
    def __str__(self):
        return "Radius %s exceeds %s" % (self.radius, self.value)

class Application(tornado.web.Application):
    def __init__(self, index=None, handlers=None, default_host="",
        transforms=None, wsgi=False, **settings):
        super(Application, self).__init__(
            handlers, default_host, transforms, wsgi, **settings)
        self.index = index

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        index = self.application.index
        self.write(dumps(dict(name=index.name, num_items=len(index.bwd))))
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

class SearchBoundsHandler(tornado.web.RequestHandler):
    def parse_coords(self):
        coords = None
        region = None
        bbox = self.get_argument('bbox', None)
        if bbox:
            coords = tuple(float(x) for x in bbox.split(','))
        else:
            geom = self.get_argument('geom', None)
            if geom:
                region = wkt.loads(geom)
                coords = region.bounds
        if coords is None:
            raise ValueError("No search bounds specified")
        elif len(coords) == 4 and options.max_bbox_area is not None:
            if geom({'bbox': coords}).area > options.max_bbox_area:
                raise SearchBoxTooLarge(bbox, options.max_bbox_area)
        self.coords = coords
        self.region = region
        return self.coords, self.region

class IntersectionHandler(SearchBoundsHandler):
    def get(self):
        '''Perform intersection search'''
        try:
            coords, region = self.parse_coords()
            # Strict -- true geometric intersection?
            strict = bool(self.get_argument('strict', 0))
            # Paging args
            start = int(self.get_argument('start', '0'))
            count = int(self.get_argument('count', '0')) or options.page_size
            if count > options.max_page_size:
                count = options.max_page_size
            # Query
            candidates = self.application.index.intersection(coords)
            if strict and region:
                def pred(item):
                    g = geom(item)
                    return region.contains
                hits = list(ifilter(pred, candidates))
            else:
                hits = list(candidates)
            results = hits[start:start+count]
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(
                dict(
                    bbox=coords, 
                    start=start, 
                    count=len(results), 
                    hits=len(hits), 
                    items=results)
                ))
        except:
            raise

class NearestHandler(SearchBoundsHandler):
    def get(self):
        '''Perform intersection search'''
        try:
            coords, region = self.parse_coords()
            limit = int(self.get_argument('limit', '1'))
            if limit > options.max_nearest_limit:
                limit = options.max_nearest_limit
            results = list(self.application.index.nearest(coords, limit))
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(
                dict(
                    bbox=coords, 
                    limit=limit, 
                    count=len(results), 
                    items=results)
                ))
        except:
            raise

class DistanceHandler(tornado.web.RequestHandler):
    def get(self):
        '''Perform distance search'''
        try:
            x = float(self.get_argument('lon'))
            y = float(self.get_argument('lat'))
            r = float(self.get_argument('radius'))
            if options.max_radius is not None:
                if r > options.max_radius:
                    raise SearchRadiusTooLarge(r, options.max_radius)
            start = int(self.get_argument('start', '0'))
            count = int(self.get_argument('count', '0')) or options.page_size
            if count > options.max_page_size: 
                count = options.max_page_size
            # Make a box for first pass intersection search
            Re = 6371000.0 # radius of earth in meters, spherical approx
            F = 180.0/(math.pi*Re)
            d = F*r/math.cos(math.pi*y/180.0)
            coords = (x-d, y-d, x+d, y+d)
            candidates = self.application.index.intersection(coords)
            # Now filter geometrically
            region = Point(x, y).buffer(d)
            def pred(ob):
                g = geom(ob)
                return region.distance(g) <= d
            hits = list(ifilter(pred, candidates))
            results = hits[start:start+count]
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(
                dict(
                    lon=x, 
                    lat=y, 
                    radius=r, 
                    start=start, 
                    count=len(results), 
                    hits=len(hits), items=results)
                ))
        except:
            raise

class SingleItemHandler(tornado.web.RequestHandler):
    def get(self, fid, minx, miny, maxx, maxy):
        '''Return representation of a stored item using its (id, bbox) key'''
        a, b = str(fid), (float(minx), float(miny), float(maxx), float(maxy))
        try:
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            self.write(dumps(self.application.index.item(a, b)))
        except:
            raise

class MultipleItemsHandler(tornado.web.RequestHandler):
    def get(self, fid):
        '''Return representation of stored items with an id'''
        try:
            self.set_status(200)
            self.set_header('content-type', 'application/json')
            items = self.application.index.items(fid)
            self.write(dumps(dict(id=fid, items=[items])))
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
    parser.add_option(
        "-p", "--port", dest="port",
        help="Port", default="0")

    opts, args = parser.parse_args(argv1)
    name = args[0]
    
    if opts.data.startswith('/'):
        instance_path = os.path.join(opts.data, name)
    else:
        instance_path = os.path.join(os.getcwd(), opts.data, name) 
    config_file = opts.config_file or os.path.join(os.path.abspath(instance_path), "etc", "vaytrou.conf")
    if config_file is not None:
        parse_config_file(config_file)    
    parse_command_line([])

    # Load command line options over Tornado options
    for k in options:
        v = getattr(opts, k, None)
        if v is not None:
            options[k].set(v)

    finder = PersistentApplicationFinder(
        'file://%s/%s/var/Data.fs' % (options.data, name), appmaker)
    index = finder(environ)
    index.fwd = Rtree('%s/%s/var/vrt1' % (options.data, name))
    setattr(index, 'name', name)

    return Application(index, [
        (r'/', MainHandler),
        (r'/intersection/?', IntersectionHandler),
        (r'/nearest/?', NearestHandler),
        (r'/distance/?', DistanceHandler),
        (r'/item/([-\w]+);([-0-9\.]+),([-0-9\.]+),([-0-9\.]+),([-0-9\.]+)', 
         SingleItemHandler),
        (r'/items/([-\w]+)', MultipleItemsHandler)
        ])



