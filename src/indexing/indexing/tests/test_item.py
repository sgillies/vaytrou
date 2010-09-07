from indexing import Item

def test_id():
    o = Item(id='1')
    assert o.id == '1'

def test_point_bbox():
    o = Item(
            id='1', 
            geometry={'type': 'Point', 'coordinates': (0.0, 0.0)})
    assert o.bbox == (0.0, 0.0, 0.0, 0.0)

def test_line_bbox():
    o = Item(
            id='1', 
            geometry={'type': 'LineString', 'coordinates': ((0.0, 0.0), (1.0, 1.0))})
    assert o.bbox == (0.0, 0.0, 1.0, 1.0)

def test_polygon_bbox():
    o = Item(
            id='1', 
            geometry={'type': 'Polygon', 'coordinates': (((-1.0, 0.0), (0.0, 1.0), (1.0, 0.0), (0.0, -1.0)),)})
    assert o.bbox == (-1.0, -1.0, 1.0, 1.0)

