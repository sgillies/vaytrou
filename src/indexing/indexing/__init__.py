#
import uuid

from indexing.bounds import bbox


class ConflictError(Exception):
    pass

def key(o):
    return (o.get('id') or str(uuid.uuid4()), tuple(o['bbox']))

class ChangeSet(object):
    """Atomic group of items to be indexed or unindexed
    """
    def __init__(self, additions=None, deletions=None):
        """If additions and deletions intersect, a ConflictError will be raised.
        """
        self.additions = [dict(a, bbox=bbox(a)) for a in additions or []]
        self.additions_made = []
        self.deletions = [dict(d, bbox=bbox(d)) for d in deletions or []]
        self.deletions_made = []
        self.no_ops = []
        if set([key(a) for a in self.additions]).intersection(
            set([key(d) for d in self.deletions])):
            raise ConflictError(
                "check intersection of additions and deletions in changeset.")

class IndexingError(Exception):
    pass

class UnindexingError(Exception):
    pass

class BatchError(Exception):
    pass

class UnrecoveredBatchError(BatchError):
    def __init__(self, msg, unrecovered_additions, unrecovered_deletions):
        self.msg = msg
        self.additions = unrecovered_additions
        self.deletions = unrecovered_deletions
    def __repr__(self):
        return '%s: %s, %s' % (self.msg, self.additions, self.deletions)

class BaseIndex(object):
    """Base class for indexes
    
    Deals in GeoJSON-like dicts we will call ``items``, like:

      {'id': 'db441f41-ec34-40fb-9f62-28b5a9f2f0e5',    # required
       'bbox': (0.0, 0.0, 1.0, 1.0),                    # recommended
       'geometry':                                      # required
         {'type': 'LineString', 'coordinates': ((0.0, 0.0), (1.0, 1.0))},
       'properties': { ... }                            # optional
       ... }

    """
    def intid(self, item):
        """Return a unique integer id for the item"""
        raise NotImplementedError
    def bbox(self, item):
        """Return a (minx, miny, maxx, maxy) tuple for the item"""
        return bbox(item)
    def intersection(self, bbox):
        """Return an iterator over items that intersect with the bbox"""
        raise NotImplementedError
    def nearest(self, bbox, limit=0):
        """Return an iterator over the nearest N=limit items to the bbox"""
        raise NotImplementedError
    def index_item(self, itemid, bbox, item):
        """Index item using unique integer itemid and bbox as key"""
        raise NotImplementedError
    def unindex_item(self, itemid, bbox):
        """Unindex the item with (itemid, bbox) as key"""
        raise NotImplementedError
    def diff(self, sa, sb):
        """Return a representation of the difference between two sequences of 
        items"""
        dd = set([tuple(a.items()) for a in sa]) \
           - set([tuple(b.items()) for b in sb])
        return [dict(d) for d in dd]
    def batch(self, changeset):
        """Execute an all-or-nothing batch of index additions and deletions"""
        # play additions and deletions
        try:
            for a in changeset.additions:
                self.index_item(self.intid(a), self.bbox(a), a)
                changeset.additions_made.append(a)
            for d in changeset.deletions:
                self.unindex_item(self.intid(d), self.bbox(d))
                changeset.deletions_made.append(d)
        except (IndexingError, UnindexingError):
            # undo
            undone_additions = undone_deletions = []
            try:
                undone_additions[:] = [
                    m for m in changeset.additions_made \
                    if not self.unindex_item(self.intid(m), self.bbox(m))]
                undone_deletions[:] = [
                    n for n in changeset.deletions_made \
                    if not self.index_item(self.intid(n), self.bbox(n), n)]
            except (IndexingError, UnindexingError):
                raise UnrecoveredBatchError(
                    "Index state not recovered.", 
                    self.diff(changeset.additions_made, undone_additions),
                    self.diff(changeset.deletions_made, undone_deletions))
            raise BatchError("Index state recovered.")
