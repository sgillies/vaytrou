from unittest import TestCase

from indexing.bounds import Bounds

class BoundsTestCase(TestCase):
    def test_base(self):
        o = Bounds()
        self.assertRaises(NotImplementedError, o.xy, None)

