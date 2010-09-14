import os
from tempfile import mkdtemp
from unittest import TestCase

from indexing import ChangeSet, Item
from repoze.zodbconn.uri import db_from_uri
from rtree import Rtree

from vrtree import VRtreeIndex

class VRtreeIndexTestCase(TestCase):
    def test_empty_changeset(self):
        idx = VRtreeIndex()
        set = ChangeSet()
        self.assertEquals(idx.batch(set), None)
    def test_additions(self):
        idx = VRtreeIndex()
        additions = [
            Item(**{'id': '1', 
                  'geometry': {'type': 'Point', 'coordinates': (0.0, 0.0)}}),
            Item(**{'id': '2', 
                  'geometry': {'type': 'Point', 'coordinates': (1.0, 1.0)}}),
            Item(**{'id': '3', 
                  'geometry': {'type': 'Point', 'coordinates': (10.01, 10.01)}})]
        set = ChangeSet(additions=additions)
        self.assertEquals(idx.batch(set), None)
        self.assertEquals(set.additions_made, set.additions)
        self.assertEquals(set.deletions_made, set.deletions)
        self.assertEquals(list(idx.intersection((-0.1, -0.1, 1.1, 1.1))), [{'geometry': {'type': 'Point', 'coordinates': (0, 0)}, 'properties': {}, 'id': '1', 'bbox': (0, 0, 0, 0)}, {'geometry': {'type': 'Point', 'coordinates': (1, 1)}, 'properties': {}, 'id': '2', 'bbox': (1, 1, 1, 1)}])
        self.assertEquals(list(idx.nearest((9.0, 9.0, 9.1, 9.1))), [{'geometry': {'type': 'Point', 'coordinates': (10.01, 10.01)}, 'properties': {}, 'id': '3', 'bbox': (10.01, 10.01, 10.01, 10.01)}])


