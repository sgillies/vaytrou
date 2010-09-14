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

class VRtreeIndex(BaseIndex):
    def __init__(self, index, zodb):
        self.rtree = index
        self.db = zodb
        conn = self.db.open()
        root = conn.root()
        if not 'app' in root:
            app = Container()
            app['fwd'] = OOBTree.OOBTree()
            app['bwd'] = IOBTree.IOBTree()
            root['app'] = app
            transaction.commit()
        self.fwd = root['app']['fwd']
        self.bwd = root['app']['bwd']
    def intersection(self, bbox):
        """Return an iterator over Items that intersect with the bbox"""
        for hit in self.rtree.intersection(bbox):
            yield self.bwd[int(hit-2**32)]
    def nearest(self, bbox, limit=1):
        """Return an iterator over the nearest N=limit Items to the bbox"""
        for hit in self.rtree.nearest(bbox, limit):
            yield self.bwd[int(hit-2**32)]
    def index_item(self, item):
        """Add an Item to the index"""
        intid = hash(item.id)
        if intid in self.bwd:
            self.unindex_item(item)
        self.rtree.add(intid + 2**32, item.bbox)
        value = map_item(item)
        self.bwd[intid] = value
        set = self.fwd.get(value)
        if set is None:
            set = IFBTree.IFTreeSet()
            self.fwd[value] = set
        set.insert(intid)
    def unindex_item(self, item):
        """Remove an Item from the index"""
        intid = hash(item.id)
        value = self.bwd.get(intid)
        if value is None:
            return
        self.fwd[value].remove(intid)
        del self.bwd[intid]
        self.rtree.delete(intid + 2**32, bbox)
    def batch(self, changeset):
        BaseIndex.batch(self, changeset)
        transaction.commit()
