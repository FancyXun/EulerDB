import unittest

from schema.metadata import Delta


class MyTestCase(unittest.TestCase):
    def test_delta_instance(self):
        delta = Delta()
        delta1 = Delta()
        self.assertEqual(delta, delta1)  # add assertion here

    def test_meta_update(self):
        delta = Delta()
        delta1 = Delta()
        delta1.update_delta()
        self.assertEqual(delta, delta1)  # add assertion here


if __name__ == '__main__':
    unittest.main()
