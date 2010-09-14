#

from geojson import Feature

from indexing.bounds import calc


class Item(Feature):
    @property
    def bbox(self):
        return calc[self.geometry.type](self.geometry)

class ConflictError(Exception):
    pass

class ChangeSet(object):
    def __init__(self, additions=None, deletions=None):
        """If additions and deletions intersect, a ConflictError will be raised.
        """
        self.additions = additions or []
        self.additions_made = []
        self.deletions = deletions or []
        self.deletions_made = []
        self.no_ops = []
        if set(self.additions).intersection(set(self.deletions)):
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
    def intersection(self, bbox):
        """Return an iterator over Items that intersect with the bbox"""
        raise NotImplementedError
    def nearest(self, bbox, limit=0):
        """Return an iterator over the nearest N=limit Items to the bbox"""
        raise NotImplementedError
    def index_item(self, itemid, item):
        """Add an Item to the index"""
        raise NotImplementedError
    def unindex_item(self, itemid):
        """Remove an Item from the index"""
        raise NotImplementedError
    def batch(self, changeset):
        """Execute an all-or-nothing batch of index additions and deletions"""
        # play additions and deletions
        try:
            for a in changeset.additions:
                itemid = self.intid(a)
                self.index_item(itemid, a)
                changeset.additions_made.append(a)
            for d in changeset.deletions:
                itemid = self.intid(d)
                self.unindex_item(itemid)
                changeset.deletions_made.append(d)
        except (IndexingError, UnindexingError):
            # undo
            undone_additions = undone_deletions = []
            try:
                undone_additions[:] = [
                    m for m in changeset.additions_made \
                    if not self.unindex_item(self.intid(m))]
                undone_deletions[:] = [
                    n for n in changeset.deletions_made \
                    if not self.index_item(self.intid(n), n)]
            except (IndexingError, UnindexingError):
                raise UnrecoveredBatchError(
                    "Index state not recovered.", 
                    set(changeset.additions_made) - set(undone_additions),
                    set(changeset.deletions_made) - set(undone_deletions))
            raise BatchError("Index state recovered.")
