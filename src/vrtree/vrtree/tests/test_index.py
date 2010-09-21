import os
from tempfile import mkdtemp
from unittest import TestCase

from indexing import ChangeSet
from repoze.zodbconn.uri import db_from_uri
from rtree import Rtree

from vrtree import IntRtreeIndex

class IntRtreeIndexTestCase(TestCase):
    def test_duplicate_index(self):
        idx = IntRtreeIndex()
        idx.index_item(1, (0, 0, 0, 0), {'foo': 'bar'})
        idx.index_item(1, (0, 0, 0, 0), {'foo': 'baz'})
        self.assertEqual(len(idx.bwd), 1)
        self.assertEqual(idx.fwd.count(idx.fwd.bounds), 1)
    def test_unindex(self):
        idx = IntRtreeIndex()
        idx.index_item(1, (0, 0, 0, 0), {'foo': 'bar'})
        self.assertEqual(len(idx.bwd), 1)
        self.assertEqual(idx.fwd.count(idx.fwd.bounds), 1)
        idx.unindex_item(1, (0, 0, 0, 0))
        self.assertEqual(len(idx.bwd), 0)
        self.assertEqual(idx.fwd.count((0, 0, 1, 1)), 0)
    def test_unindex_nothing(self):
        assert IntRtreeIndex().unindex_item(1, (0, 0, 0, 0)) is None
    def test_batch_empty(self):
        idx = IntRtreeIndex()
        set = ChangeSet()
        self.assertEquals(idx.batch(set), None)
    def test_batch_adds(self):
        idx = IntRtreeIndex()
        additions = [
            {'id': '1', 'bbox': (0, 0, 0, 0), 't': 'One'},
            {'id': '2', 'geometry': {'type': 'Point', 'coordinates': (0.0, 0.0)},
             't': 'Two'},
            {'id': '3', 
             'geometry': {'type': 'Point', 'coordinates': (10.01, 10.01)}, 
             't': 'Three'}]
        set = ChangeSet(additions=additions)
        self.assertEquals(idx.batch(set), None)
        self.assertEquals(set.additions_made, set.additions)
        self.assertEquals(set.deletions_made, set.deletions)
        self.assertEquals(list(idx.intersection((-0.1, -0.1, 1.1, 1.1))), [{'t': 'One', 'bbox': (0, 0, 0, 0), 'id': '1'}, {'geometry': {'type': 'Point', 'coordinates': (0.0, 0.0)}, 'id': '2', 'bbox': (0.0, 0.0, 0.0, 0.0), 't': 'Two'}])
        self.assertEquals(list(idx.nearest((9.0, 9.0, 9.1, 9.1))), [{'geometry': {'type': 'Point', 'coordinates': (10.01, 10.01)}, 'id': '3', 'bbox': (10.01, 10.01, 10.01, 10.01), 't': 'Three'}])
    def test_batch_deletes(self):
        idx = IntRtreeIndex()
        additions1 = [
            {'id': '1', 'bbox': (0, 0, 0, 0), 't': 'One'},
            {'id': '2', 'geometry': {'type': 'Point', 'coordinates': (0.0, 0.0)},
             't': 'Two'}]
        set1 = ChangeSet(additions=additions1)
        self.assertEquals(idx.batch(set1), None)
        additions2 = [
            {'id': '3', 
             'geometry': {'type': 'Point', 'coordinates': (10.01, 10.01)}, 
             't': 'Three'}]
        deletions2 = [{'id': '1', 'bbox': (0, 0, 0, 0)}]
        set2 = ChangeSet(additions2, deletions2)
        self.assertEquals(idx.batch(set2), None)
        self.assertEquals(list(idx.intersection((-0.1, -0.1, 1.1, 1.1))), [{'geometry': {'type': 'Point', 'coordinates': (0.0, 0.0)}, 'id': '2', 'bbox': (0.0, 0.0, 0.0, 0.0), 't': 'Two'}])
        self.assertEquals(list(idx.nearest((9.0, 9.0, 9.1, 9.1))), [{'geometry': {'type': 'Point', 'coordinates': (10.01, 10.01)}, 'id': '3', 'bbox': (10.01, 10.01, 10.01, 10.01), 't': 'Three'}])
