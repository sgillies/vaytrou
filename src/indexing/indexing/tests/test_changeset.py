from unittest import TestCase

from indexing import ChangeSet

class ChangeSetTestCase(TestCase):
    def test_init(self):
        set = ChangeSet()
        self.assertEqual(set.additions, [])
        self.assertEqual(set.additions_made, [])
        self.assertEqual(set.deletions, [])
        self.assertEqual(set.deletions_made, [])
        self.assertEqual(set.no_ops, [])

