import unittest
try:
    from unittest import mock
except ImportError:
    import mock

from .helpers import ComponentTest


class RepeatTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


class DropTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


class SleepTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


# class SplitTest(ComponentTest):
#     @unittest.skip('unimplemented')
#     def test_component(self):
#         pass


class RegexFilterTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


# class ConcatTest(ComponentTest):
#     @unittest.skip('unimplemented')
#     def test_component(self):
#         pass


class MultiplyTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


class FileTailReaderTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


class ConsoleLineWriterTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


# class LogTapTest(ComponentTest):
#     @unittest.skip('unimplemented')
#     def test_component(self):
#         pass


class RandomNumberGeneratorTest(ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass
