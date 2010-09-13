from unittest import TestCase

from indexing import ChangeSet, Index, IndexingError, UnindexingError
from indexing import BatchError, UnrecoveredBatchError

class AnyIndex(Index):
    def index_item(self, item):
        pass
    def unindex_item(self, item):
        pass

class BatchingTestCase(TestCase):
    def test_empty_changeset(self):
        idx = AnyIndex(None)
        set = ChangeSet()
        self.assertEquals(idx.batch(set), 1)
    def test_noproblem_changeset(self):
        idx = AnyIndex(None)
        set = ChangeSet(additions=[1,2,3], deletions=[4,5,6])
        self.assertEquals(idx.batch(set), 1)
        self.assertEquals(set.additions_made, set.additions)
        self.assertEquals(set.deletions_made, set.deletions)

class UnluckyIndex(Index):
    def index_item(self, item):
        if item == 13:
            raise IndexingError
    def unindex_item(self, item):
        if item == 13:
            raise IndexingError

class UnluckyBatchingTestCase(TestCase):
    def test_addition_problem(self):
        idx = UnluckyIndex(None)
        set = ChangeSet(additions=[1,13,3], deletions=[4,5,6])
        self.assertRaises(BatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [1])
        self.assertEquals(set.deletions_made, [])
    def test_deletion_problem(self):
        idx = UnluckyIndex(None)
        set = ChangeSet(additions=[1,2,3], deletions=[4,13,6])
        self.assertRaises(BatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [1, 2, 3])
        self.assertEquals(set.deletions_made, [4])

class HotelCaliforniaIndex(Index):
    def index_item(self, item):
        pass
    def unindex_item(self, item):
        if item == 13:
            raise IndexingError

class UnrecoverableBatchingTestCase(TestCase):
    def test_unrecoverable(self):
        idx = HotelCaliforniaIndex(None)
        set = ChangeSet(additions=[1,2,13], deletions=[4,13,6])
        self.assertRaises(UnrecoveredBatchError, idx.batch, set)
        self.assertEquals(set.additions_made, [1, 2, 13])
        self.assertEquals(set.deletions_made, [4])
        
