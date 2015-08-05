import unittest
try:
    from unittest import mock
except ImportError:
    import mock


class RuntimeTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_all_component_specs(self):
        pass

    @unittest.skip('unimplemented')
    def test_register_component(self):
        pass

    @unittest.skip('unimplemented')
    def test_register_module(self):
        pass

    @unittest.skip('unimplemented')
    def test_is_started(self):
        pass

    @unittest.skip('unimplemented')
    def test_start(self):
        pass

    @unittest.skip('unimplemented')
    def test_stop(self):
        pass

    @unittest.skip('unimplemented')
    def test_new_graph(self):
        pass

    @unittest.skip('unimplemented')
    def test_add_node(self):
        pass

    @unittest.skip('unimplemented')
    def test_remove_node(self):
        pass

    @unittest.skip('unimplemented')
    def test_add_edge(self):
        pass

    @unittest.skip('unimplemented')
    def test_remove_edge(self):
        pass

    @unittest.skip('unimplemented')
    def test_add_iip(self):
        pass

    @unittest.skip('unimplemented')
    def test_remove_iip(self):
        pass


class FlowhubRegistryTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_register_runtime(self):
        pass

    @unittest.skip('unimplemented')
    def test_ping_runtime(self):
        pass
