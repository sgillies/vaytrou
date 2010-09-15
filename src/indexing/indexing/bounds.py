
def coordinates(o):
    return getattr(o, 'coordinates', o.get('coordinates'))

class Bounds(object):
    def xy(self, o):
        """Return two lists of values from which to get mins and maxes"""
        raise NotImplementedError
    def __call__(self, o):
        x, y = self.xy(o)
        return min(x), min(y), max(x), max(y)

class Bounds0(Bounds):
    """Calculate bounds of 0-dimensional objects"""
    def xy(self, o):
        return zip(coordinates(o))

class Bounds1(Bounds):
    """Calculate bounds of 1-dimensional objects"""
    def xy(self, o):
        return zip(*coordinates(o))

class Bounds2(Bounds):
    """Calculate bounds of 2-dimensional objects"""
    def xy(self, o):
        x = []
        y = []
        for part in coordinates(o):
            a, b = zip(*part)
            x += [min(a), max(a)]
            y += [min(b), max(b)]
        return x, y

class Bounds2M(Bounds):
    def xy(self, o):
        x = []
        y = []
        for p in coordinates(o):
            for q in p:
                a, b = zip(*q)
                x += [min(a), max(a)]
                y += [min(b), max(b)]
        return x, y

calc = {
    'Point': Bounds0(),
    'LineString': Bounds1(),
    'Polygon': Bounds2(),
    'MultiPoint': Bounds1(),
    'MultiLineString': Bounds2(),
    'MultiPolygon': Bounds2M()
    }

