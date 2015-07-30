from .base import GraphRuntime
from ..core import ComponentState
from ..exc import GraphRuntimeError


# TODO: implement this
class DistributedGraphRuntime(GraphRuntime):
    """
    Executes a graph in parallel using multiple processes that may reside on multiple
    machines over a network, where each component is run in its own process.

    This runtime is more scalable than the MultiProcessRuntime, but it comes with more
    overhead in terms of execution and administration.
    """
    def __init__(self, graph):
        super(DistributedGraphRuntime, self).__init__(graph)

    def execute(self):
        raise NotImplementedError

    def send_port(self, component, port_name, packet):
        raise NotImplementedError

    def receive_port(self, component, port_name):
        raise NotImplementedError

    def close_input_port(self, component, port_name):
        raise NotImplementedError

    def close_output_port(self, component, port_name):
        raise NotImplementedError

    def terminate_thread(self, component):
        raise NotImplementedError

    def suspend_thread(self, seconds=None):
        raise NotImplementedError
