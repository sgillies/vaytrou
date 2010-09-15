#
from BTrees import IFBTree, IOBTree, OOBTree
from persistent.mapping import PersistentMapping
from rtree import Rtree
import transaction

from indexing import BaseIndex
from indexing.bounds import calc

#class Container(PersistentMapping):
#    __parent__ = __name__ = None

#def map_item(item):
#    return PersistentMapping(geo_interface(item))
#def persist(item):
#    return PersistentMapping(item)

OFFSET = 2**32

class VRtreeIndex(BaseIndex):
    """Rtree requires unsigned long ids. Python's hash() yields signed ints,
    so we add 2**32 to hashed values for the former, and subtract the same from
    results of rtree methods."""
    def clear(self):
        self.bwd = OOBTree.OOBTree()
        self.fwd = Rtree()
    def __init__(self):
        self.clear()
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
        return hash(item['id']) + OFFSET
    def intersection(self, bbox):
        """Return an iterator over Items that intersect with the bbox"""
        for hit in self.fwd.intersection(bbox, True):
            yield self.bwd[(hit.id, tuple(hit.bbox))]
    def nearest(self, bbox, limit=1):
        """Return an iterator over the nearest N=limit Items to the bbox"""
        for hit in self.fwd.nearest(bbox, limit, True):
            yield self.bwd[(hit.id, tuple(hit.bbox))]
    def index_item(self, itemid, bbox, item):
        """Add an Item to the index"""
        if (itemid, bbox) in self.bwd:
            self.unindex_item(itemid, bbox)
        self.fwd.add(itemid, bbox)
        self.bwd[(itemid, bbox)] = item
        #set = self.fwd.get(value)
        #if set is None:
        #    set = IFBTree.IFTreeSet()
        #    self.fwd[value] = set
        #set.insert(itemid)
    def unindex_item(self, itemid, bbox):
        """Remove an Item from the index"""
        #import pdb; pdb.set_trace()
        value = self.bwd.get((itemid, bbox))
        if value is None:
            return
        #self.fwd[value].remove(itemid)
        import pdb; pdb.set_trace()
        del self.bwd[(itemid, bbox)]
        self.fwd.delete(itemid, bbox)
    def batch(self, changeset):
        BaseIndex.batch(self, changeset)
        transaction.commit()
