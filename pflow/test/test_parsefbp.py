import unittest
try:
    from unittest import mock
except ImportError:
    import mock


class ParserTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_foo(self):
        pass
