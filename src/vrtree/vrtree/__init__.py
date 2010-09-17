#
from BTrees import OOBTree
from rtree import Rtree
import transaction

from indexing import BaseIndex

OFFSET = 2**32

class RtreeIndex(BaseIndex):
    """An index with an R-tree as the forward mapping and a B-tree as the
    backward mapping

    Modeled after the example at http://docs.zope.org/zope.catalog/index.html.
    """
    def clear(self):
        self.fwd = Rtree()
        self.bwd = OOBTree.OOBTree()
    def __init__(self):
        self.clear()
    def intid(self, item):
        """Rtree requires unsigned long ids. Python's hash() yields signed ints,
        so we add 2**32 to hashed values for the former."""
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
    def unindex_item(self, itemid, bbox):
        """Remove an Item from the index"""
        value = self.bwd.get((itemid, bbox))
        if value is None:
            return
        del self.bwd[(itemid, bbox)]
        self.fwd.delete(itemid, bbox)
    def batch(self, changeset):
        BaseIndex.batch(self, changeset)
        transaction.commit()

class VRtreeIndex(RtreeIndex):
    """Variant that expects the foward Rtree to be set by a caller"""
    def clear(self):
        self.fwd = None
        self.bwd = OOBTree.OOBTree()
    
