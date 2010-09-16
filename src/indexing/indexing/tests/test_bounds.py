from unittest import TestCase

from indexing.bounds import Bounds, calc

class BoundsTestCase(TestCase):
    def test_base(self):
        o = Bounds()
        self.assertRaises(NotImplementedError, o.xy, None)

def test_point_bbox():
    o = {'type': 'Point', 'coordinates': (0.0, 0.0)}
    assert calc['Point'](o) == (0.0, 0.0, 0.0, 0.0)

def test_line_bbox():
    o = {'type': 'LineString', 'coordinates': ((0.0, 0.0), (1.0, 1.0))}
    assert calc['LineString'](o) == (0.0, 0.0, 1.0, 1.0)

def test_polygon_bbox():
    o = {'type': 'Polygon', 'coordinates': (((-1.0, 0.0), (0.0, 1.0), (1.0, 0.0), (0.0, -1.0)),)}
    assert calc['Polygon'](o) == (-1.0, -1.0, 1.0, 1.0)

def test_multipolygon_bbox():
    o = {'type': 'MultiPolygon', 'coordinates': ((((-1.0, 0.0), (0.0, 1.0), (1.0, 0.0), (0.0, -1.0)),),)}
    assert calc['MultiPolygon'](o) == (-1.0, -1.0, 1.0, 1.0)



