from unittest import TestCase

from indexing import ChangeSet, Index

class MockIndex(Index):
    def index_item(self, item):
        pass
    def unindex_item(self, item):
        pass

class BatchingTestCase(TestCase):
    def test_empty_changeset(self):
        idx = MockIndex(None)
        set = ChangeSet()
        self.assertEquals(idx.batch(set), 1)
    def test_noproblem_changeset(self):
        idx = MockIndex(None)
        set = ChangeSet(additions=[1,2,3], deletions=[4,5,6])
        self.assertEquals(idx.batch(set), 1)
        self.assertEquals(set.additions_made, set.additions)
        self.assertEquals(set.deletions_made, set.deletions)

