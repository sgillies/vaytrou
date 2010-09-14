#
from BTrees import IFBTree, IOBTree, OOBTree
from persistent.mapping import PersistentMapping
from rtree import Rtree
import transaction

from indexing import BaseIndex

class Container(PersistentMapping):
    __parent__ = __name__ = None

def map_item(item):
    return PersistentMapping(
            id=item.id, 
            bbox=item.bbox, 
            geometry=PersistentMapping(
                type=item.geometry.type, 
                coordinates=item.geometry.coordinates),
            properties=PersistentMapping(item.properties))

OFFSET = 2**32

class VRtreeIndex(BaseIndex):
    """Rtree requires unsigned long ids. Python's hash() yields signed ints,
    so we add 2**32 to hashed values for the former, and subtract the same from
    results of rtree methods."""
    def clear(self):
        self.fwd = OOBTree.OOBTree()
        self.bwd = IOBTree.IOBTree()
        self.rtree = Rtree()
    def __init__(self):
        self.clear()
#
#        self.rtree = index
#        self.db = zodb
#        conn = self.db.open()
#        root = conn.root()
#        if not 'app' in root:
#            app = Container()
#            app['fwd'] = OOBTree.OOBTree()
#            app['bwd'] = IOBTree.IOBTree()
#            root['app'] = app
#            transaction.commit()
#        self.fwd = root['app']['fwd']
#        self.bwd = root['app']['bwd']a
    def intid(self, item):
        return hash(item)
    def intersection(self, bbox):
        """Return an iterator over Items that intersect with the bbox"""
        for hit in self.rtree.intersection(bbox):
            yield self.bwd[int(hit-OFFSET)]
    def nearest(self, bbox, limit=1):
        """Return an iterator over the nearest N=limit Items to the bbox"""
        for hit in self.rtree.nearest(bbox, limit):
            yield self.bwd[int(hit-OFFSET)]
    def index_item(self, itemid, item):
        """Add an Item to the index"""
        if itemid in self.bwd:
            self.unindex_item(itemid, item)
        self.rtree.add(itemid + OFFSET, item.bbox)
        value = map_item(item)
        self.bwd[itemid] = value
        set = self.fwd.get(value)
        if set is None:
            set = IFBTree.IFTreeSet()
            self.fwd[value] = set
        set.insert(itemid)
    def unindex_item(self, itemid):
        """Remove an Item from the index"""
        value = self.bwd.get(itemid)
        if value is None:
            return
        self.fwd[value].remove(itemid)
        del self.bwd[itemid]
        self.rtree.delete(itemid + OFFSET, value.get('bbox'))
    def batch(self, changeset):
        BaseIndex.batch(self, changeset)
        transaction.commit()
