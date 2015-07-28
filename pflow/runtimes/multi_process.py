import logging
import multiprocessing as mp

from .base import Runtime

log = logging.getLogger(__name__)


class MultiProcessRuntime(Runtime):
    '''
    Processes are preemtively multitasked, since each is a multiprocessing.Process.

    Uses either a multiprocessing.Queue or a distributed message queue for Packet buffering.
    Execution is suspended in one of these cases:
        1) suspend_thread() is called
        2) OS kernel preempts another running process.
    '''
    def execute_graph(self, graph):
        self._inject_runtime(graph)

        raise NotImplementedError

    def send(self, packet, dest_port):
        raise NotImplementedError

    def receive(self, source_port):
        raise NotImplementedError

    def port_has_data(self, port):
        raise NotImplementedError

    def clear_port(self, port):
        raise NotImplementedError

    def terminate_thread(self, seconds=None):
        raise NotImplementedError

    def suspend_thread(self, seconds=None):
        raise NotImplementedError
