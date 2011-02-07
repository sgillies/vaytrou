
import logging
import os
import random
from rtree import Rtree
import shutil
import uuid

import BTrees
from BTrees.IIBTree import union, IISet
import transaction

from indexing import BaseIndex

log = logging.getLogger('vrtree')

class IntRtreeIndex(BaseIndex):
    """Avoids the slower Rtree query object=True interface
    """
    _v_nextuid = None
    family = BTrees.family32

    def clear(self):
        self.fwd = Rtree()
        self.bwd = self.family.OO.BTree()
        self.keys = self.family.IO.BTree()
        self.intids = self.family.OI.BTree()
        self.ids = self.family.OO.BTree()
    def __init__(self):
        self.clear()
    def key(self, item):
        try:
            return item['id'], tuple(self.bbox(item))
        except:
            return tuple(item.items())
    def fid(self, item):
        return item['id']
    def intid(self, item):
        # Get and track next available key using zope.intid algorithm
        # Item might be already registered
        uid = self.intids.get(self.key(item))
        if uid is not None:
            return uid
        # But if not registered
        nextuid = getattr(self, '_v_nextuid', None)
        while True:
            if nextuid is None:
                nextuid = random.randrange(0, self.family.maxint)
            uid = nextuid
            if uid not in self.keys:
                nextuid += 1
                if nextuid > self.family.maxint:
                    nextuid = None
                self._v_nextuid = nextuid
                return uid
            nextuid = None
    def intersection(self, bbox):
        """Return an iterator over Items that intersect with the bbox"""
        for hit in self.fwd.intersection(bbox, objects=False):
            yield self.bwd[int(hit)]
    def nearest(self, bbox, limit=1):
        """Return an iterator over the nearest N=limit Items to the bbox"""
        for hit in self.fwd.nearest(bbox, num_results=limit, objects=False):
            yield self.bwd[int(hit)]
    def item(self, fid, bbox):
        return self.bwd[self.intids[(fid, bbox)]]
    def items(self, fid):
        return [self.bwd[intid] for intid in self.ids[fid]]
    def index_item(self, itemid, bbox, item):
        """Add an Item to the index"""
        if itemid in self.bwd:
            self.unindex_item(itemid, bbox)
        # Store an id for the item if it has None
        try:
            item.update(id=item.get('id') or str(uuid.uuid4()))
            key = self.key(item)
            sid = self.fid(item)
            
            # Map keys <-> intids
            intid = self.intid(item)
            self.keys[intid] = key
            self.intids[key] = intid
            
            if sid not in self.ids:
                self.ids[sid] = IISet([])
            self.ids[sid].add(intid)

            self.bwd[intid] = item
            self.fwd.add(intid, bbox)
        except:
            import pdb; pdb.set_trace()
            raise
    def unindex_item(self, itemid, bbox):
        """Remove an Item from the index"""
        intid = self.intid(item)
        key = self.keys.get(intid)
        if key is None:
            return
        self.ids[key[0]].remove(intid)
        del self.keys[intid]
        del self.intids[key]
        del self.bwd[intidd]
        self.fwd.delete(intid, bbox)
    def batch(self, changeset):
        BaseIndex.batch(self, changeset)
    def commit(self):
        transaction.commit()
        rtree_storage = self.fwd.properties.filename
        self.fwd.close()
        self.fwd = Rtree(rtree_storage)
    def close(self):
        self.fwd.close()

class VIntRtreeIndex(IntRtreeIndex):
    """Variant that expects the foward Rtree to be set by a caller"""
    def clear(self):
        self.fwd = None
        self.bwd = self.family.OO.BTree()
        self.keys = self.family.IO.BTree()
        self.intids = self.family.OI.BTree()
        self.ids = self.family.OO.BTree()

