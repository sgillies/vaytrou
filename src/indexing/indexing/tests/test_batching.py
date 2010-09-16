from unittest import TestCase

from indexing import ChangeSet, BaseIndex, IndexingError, UnindexingError
from indexing import BatchError, UnrecoveredBatchError

class AnyIndex(BaseIndex):
    def intid(self, item):
        return int(item['id'])
    def bbox(self, item):
        return (0, 0, 1, 1)
    def index_item(self, itemid, bbox, item):
        pass
    def unindex_item(self, itemid, bbox):
        pass

def f(id):
    return dict(id=id, bbox=(0, 0, 1, 1))

class BatchingTestCase(TestCase):
    def test_empty_changeset(self):
        idx = AnyIndex()
        set = ChangeSet()
        self.assertEquals(idx.batch(set), None)
    def test_noproblem_changeset(self):
        idx = AnyIndex()
        set = ChangeSet(additions=[f(1),f(2),f(3)], deletions=[f(4),f(5),f(6)])
        self.assertEquals(idx.batch(set), None)
        self.assertEquals(set.additions_made, set.additions)
        self.assertEquals(set.deletions_made, set.deletions)

class UnluckyIndex(AnyIndex):
    def index_item(self, itemid, bbox, item):
        if itemid == 13:
            raise IndexingError
    def unindex_item(self, itemid, bbox):
        if itemid == 13:
            raise IndexingError

class UnluckyBatchingTestCase(TestCase):
    def test_addition_problem(self):
        idx = UnluckyIndex()
        set = ChangeSet(additions=[f(1),f(13),f(3)], deletions=[f(4),f(5),f(6)])
        self.assertRaises(BatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [f(1)])
        self.assertEquals(set.deletions_made, [])
    def test_deletion_problem(self):
        idx = UnluckyIndex()
        set = ChangeSet(additions=[f(1),f(2),f(3)], deletions=[f(4),f(13),f(6)])
        self.assertRaises(BatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [f(1), f(2), f(3)])
        self.assertEquals(set.deletions_made, [f(4)])

class HotelCaliforniaIndex(AnyIndex):
    def index_item(self, itemid, bbox, item):
        pass
    def unindex_item(self, itemid, bbox):
        if itemid in [13, 14]:
            raise IndexingError

class UnrecoverableBatchingTestCase(TestCase):
    def test_unrecoverable(self):
        idx = HotelCaliforniaIndex()
        set = ChangeSet(additions=[f(1),f(2),f(13)], deletions=[f(4),f(14),f(6)])
        self.assertRaises(UnrecoveredBatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [f(1), f(2), f(13)])
        self.assertEquals(set.deletions_made, [f(4)])
        
