import unittest
try:
    from unittest import mock
except ImportError:
    import mock

from . import helpers


class ComponentTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_state(self):
        pass

    @unittest.skip('unimplemented')
    def test_destroy(self):
        pass

    @unittest.skip('unimplemented')
    def test_create_packet(self):
        pass

    @unittest.skip('unimplemented')
    def test_drop_packet(self):
        pass

    @unittest.skip('unimplemented')
    def test_is_terminated(self):
        pass

    @unittest.skip('unimplemented')
    def test_is_suspended(self):
        pass

    @unittest.skip('unimplemented')
    def test_terminate(self):
        pass

    @unittest.skip('unimplemented')
    def test_suspend(self):
        pass

    @unittest.skip('unimplemented')
    def test_str(self):
        pass


class InitialPacketGeneratorTest(helpers.ComponentTest):
    @unittest.skip('unimplemented')
    def test_component(self):
        pass


class GraphTest(unittest.TestCase):
    @unittest.skip('unimplemented')
    def test_get_upstream(self):
        pass

    @unittest.skip('unimplemented')
    def test_get_downstream(self):
        pass

    @unittest.skip('unimplemented')
    def test_is_upstream_terminated(self):
        pass

    @unittest.skip('unimplemented')
    def test_add_component(self):
        pass

    @unittest.skip('unimplemented')
    def test_remove_component(self):
        pass

    @unittest.skip('unimplemented')
    def test_set_initial_packet(self):
        pass

    @unittest.skip('unimplemented')
    def test_unset_initial_packet(self):
        pass

    @unittest.skip('unimplemented')
    def test_set_port_defaults(self):
        pass

    @unittest.skip('unimplemented')
    def test_connect(self):
        pass

    @unittest.skip('unimplemented')
    def test_disconnect(self):
        pass

    @unittest.skip('unimplemented')
    def test_self_starters(self):
        pass

    @unittest.skip('unimplemented')
    def test_run(self):
        pass

    @unittest.skip('unimplemented')
    def test_is_terminated(self):
        pass

    @unittest.skip('unimplemented')
    def test_load_fbp_string(self):
        pass

    @unittest.skip('unimplemented')
    def test_load_fbp_file(self):
        pass

    @unittest.skip('unimplemented')
    def test_load_json_dict(self):
        pass

    @unittest.skip('unimplemented')
    def test_load_json_file(self):
        pass

    @unittest.skip('unimplemented')
    def test_write_graphml(self):
        pass
