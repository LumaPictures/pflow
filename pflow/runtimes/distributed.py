from .base import Runtime
from ..core import ComponentState


# TODO: implement this
class DistributedRuntime(Runtime):
    """
    Executes a graph in parallel using multiple processes that may reside on multiple
    machines over a network, where each component is run in its own process.

    This runtime is more scalable than the MultiProcessRuntime, but it comes with more
    overhead in terms of execution and administration.
    """
    def execute_graph(self, graph):
        raise NotImplementedError

    def send(self, packet, dest_port):
        raise NotImplementedError

    def receive(self, source_port):
        raise NotImplementedError

    def port_has_data(self, port):
        raise NotImplementedError

    def clear_port(self, port):
        raise NotImplementedError

    def terminate_thread(self):
        raise NotImplementedError

    def suspend_thread(self, seconds=None):
        raise NotImplementedError
