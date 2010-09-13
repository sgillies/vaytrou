from unittest import TestCase

from indexing import ChangeSet, ConflictError

class ChangeSetTestCase(TestCase):
    def test_init(self):
        set = ChangeSet()
        self.assertEqual(set.additions, [])
        self.assertEqual(set.additions_made, [])
        self.assertEqual(set.deletions, [])
        self.assertEqual(set.deletions_made, [])
        self.assertEqual(set.no_ops, [])
    def test_catch_conflicts(self):
        self.assertRaises(ConflictError, ChangeSet, [1,2,3], [5,4,3])

