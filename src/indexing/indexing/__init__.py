#

from geojson import Feature

from indexing.bounds import calc


class Item(Feature):
    @property
    def bbox(self):
        return calc[self.geometry.type](self.geometry)

class ConflictError(Exception):
    pass

def bbox(o):
    return o.get('bbox') or calc[o['geometry']['type']](o['geometry'])

def f(o):
    return dict(o, bbox=bbox(o))

def key(o):
    return (o['id'], o['bbox'])

class ChangeSet(object):
    def __init__(self, additions=None, deletions=None):
        """If additions and deletions intersect, a ConflictError will be raised.
        """
        self.additions = [f(a) for a in additions or []]
        self.additions_made = []
        self.deletions = [f(d) for d in deletions or []]
        self.deletions_made = []
        self.no_ops = []
        if set([key(a) for a in self.additions]).intersection(set([key(d) for d in self.deletions])):
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

class BaseIndex(object):
    #def __init__(self):
    #    self.index = index
    def intid(self, item):
        raise NotImplementedError
    def bbox(self, item):
        return bbox(item)
    def intersection(self, bbox):
        """Return an iterator over Items that intersect with the bbox"""
        raise NotImplementedError
    def nearest(self, bbox, limit=0):
        """Return an iterator over the nearest N=limit Items to the bbox"""
        raise NotImplementedError
    def index_item(self, itemid, bbox, item):
        """Add an Item to the index"""
        raise NotImplementedError
    def unindex_item(self, itemid, bbox):
        """Remove an Item from the index"""
        raise NotImplementedError
    def batch(self, changeset):
        """Execute an all-or-nothing batch of index additions and deletions"""
        # play additions and deletions
        try:
            for a in changeset.additions:
                self.index_item(self.intid(a), self.bbox(a), a)
                changeset.additions_made.append(a)
            for d in changeset.deletions:
                self.unindex_item(self.intid(a), self.bbox(a))
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
                    set(changeset.additions_made) - set(undone_additions),
                    set(changeset.deletions_made) - set(undone_deletions))
            raise BatchError("Index state recovered.")
