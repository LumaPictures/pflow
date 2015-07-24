import unittest
try:
    from unittest import mock
except ImportError:
    import mock


class GraphTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_works(self):
        pass

