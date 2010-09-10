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
        except:
            undone_additions = [
                    m for m in changeset.additions_made \
                    if not self.unindex_item(m)]
            undone_deletions = [
                    n for n in changeset.deletions_made \
                    if not self.unindex_item(n)]
            assert m == changesets.additions_made
            assert n == changesets.deletions_made
            raise
        return 1 
