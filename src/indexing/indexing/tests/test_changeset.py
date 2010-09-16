from unittest import TestCase

from indexing import ChangeSet, ConflictError

def f(id):
    return dict(id=id, bbox=(0, 0, 1, 1))

class ChangeSetTestCase(TestCase):
    def test_init(self):
        set = ChangeSet()
        self.assertEqual(set.additions, [])
        self.assertEqual(set.additions_made, [])
        self.assertEqual(set.deletions, [])
        self.assertEqual(set.deletions_made, [])
        self.assertEqual(set.no_ops, [])
    def test_catch_conflicts(self):
        self.assertRaises(ConflictError, ChangeSet, [f(1),f(2),f(3)], [f(5),f(4),f(3)])

