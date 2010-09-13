#

from geojson import Feature

from indexing.bounds import calc


class Item(Feature):
    @property
    def bbox(self):
        return calc[self.geometry.type](self.geometry)

class ChangeSet(object):
    def __init__(self, additions=None, deletions=None):
        self.additions = additions or []
        self.additions_made = []
        self.deletions = deletions or []
        self.deletions_made = []
        self.no_ops = []

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

class Index(object):
    def __init__(self, index):
        self.index = index
    def index_item(self, item):
        raise NotImplementedError
    def unindex_item(self, item):
        raise NotImplementedError
    def batch(self, changeset):
        # play additions and deletions
        try:
            for a in changeset.additions:
                self.index_item(a)
                changeset.additions_made.append(a)
            for d in changeset.deletions:
                self.unindex_item(d)
                changeset.deletions_made.append(d)
        except (IndexingError, UnindexingError):
            # undo
            try:
                undone_additions = [
                    m for m in changeset.additions_made \
                    if not self.unindex_item(m)]
                undone_deletions = [
                    n for n in changeset.deletions_made \
                    if not self.index_item(n)]
            except (IndexingError, UnindexingError):
                raise UnrecoveredBatchError(
                    "Index state not recovered.", 
                    set(changeset.additions_made) - set(undone_additions),
                    set(changeset.deletions_made) - set(undone_deletions))
            raise BatchError("Index state recovered.")
        return 1 
