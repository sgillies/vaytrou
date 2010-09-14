from unittest import TestCase

from indexing import ChangeSet, BaseIndex, IndexingError, UnindexingError
from indexing import BatchError, UnrecoveredBatchError

class AnyIndex(BaseIndex):
    def intid(self, item):
        return int(item)
    def index_item(self, itemid, item):
        pass
    def unindex_item(self, itemid):
        pass

class BatchingTestCase(TestCase):
    def test_empty_changeset(self):
        idx = AnyIndex()
        set = ChangeSet()
        self.assertEquals(idx.batch(set), None)
    def test_noproblem_changeset(self):
        idx = AnyIndex()
        set = ChangeSet(additions=[1,2,3], deletions=[4,5,6])
        self.assertEquals(idx.batch(set), None)
        self.assertEquals(set.additions_made, set.additions)
        self.assertEquals(set.deletions_made, set.deletions)

class UnluckyIndex(AnyIndex):
    def index_item(self, itemid, item):
        if itemid == 13:
            raise IndexingError
    def unindex_item(self, itemid):
        if itemid == 13:
            raise IndexingError

class UnluckyBatchingTestCase(TestCase):
    def test_addition_problem(self):
        idx = UnluckyIndex()
        set = ChangeSet(additions=[1,13,3], deletions=[4,5,6])
        self.assertRaises(BatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [1])
        self.assertEquals(set.deletions_made, [])
    def test_deletion_problem(self):
        idx = UnluckyIndex()
        set = ChangeSet(additions=[1,2,3], deletions=[4,13,6])
        self.assertRaises(BatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [1, 2, 3])
        self.assertEquals(set.deletions_made, [4])

class HotelCaliforniaIndex(AnyIndex):
    def index_item(self, itemid, item):
        pass
    def unindex_item(self, itemid):
        if itemid in [13, 14]:
            raise IndexingError

class UnrecoverableBatchingTestCase(TestCase):
    def test_unrecoverable(self):
        idx = HotelCaliforniaIndex()
        set = ChangeSet(additions=[1,2,13], deletions=[4,14,6])
        self.assertRaises(UnrecoveredBatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [1, 2, 13])
        self.assertEquals(set.deletions_made, [4])
        
