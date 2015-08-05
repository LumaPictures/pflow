import unittest
try:
    from unittest import mock
except ImportError:
    import mock


class SingleProcessExecutorTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_foo(self):
        pass


class MultiProcessExecutorTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_foo(self):
        pass
