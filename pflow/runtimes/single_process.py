import sys
if 'threading' in sys.modules:
    import gevent.threading
    import threading
    if gevent.threading.Lock != threading.Lock:
        raise RuntimeError('threading module was imported before gevent could monkey patch it!')

import gevent.monkey
gevent.monkey.patch_all(socket=True,  # socket
                        dns=True,  # socket dns functions
                        time=True,  # time.sleep
                        select=True,  # select
                        aggressive=True,  # select/socket
                        thread=True,  # thread, threading
                        os=True,  # os.fork
                        ssl=True,
                        httplib=False,
                        subprocess=True,
                        sys=False,  # stdin, stdout, stderr
                        Event=False)

import collections

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

import gevent

from .base import Runtime
from ..core import ComponentState


class SingleProcessRuntime(Runtime):
    """
    Executes a graph in a single process, where each component is run in its own gevent coroutine.

    This low-overhead runtime is useful in environments that are hostile to multiprocessing/threading,
    or when components are mostly I/O bound (i.e. don't need process parallelization).
    """
    def __init__(self):
        super(SingleProcessRuntime, self).__init__()
        self._recv_queues = collections.defaultdict(queue.Queue)

    def execute_graph(self, graph):
        self.log.debug('Executing graph %s' % graph)
        self.inject_runtime(graph)

        coroutines = dict([(gevent.spawn(self.create_component_runner(c)), c)
                           for c in graph.components])

        def thread_error_handler(coroutine):
            """
            Handles component coroutine exceptions that get raised.
            This should terminate execution of all other coroutines.
            """
            component = coroutines[coroutine]
            self.log.error('Component "%s" failed with %s: %s' % (component.name,
                                                                  coroutine.exception.__class__.__name__,
                                                                  coroutine.exception.message))

            gevent.killall(coroutines.keys())

        # Wire up error handler (so that exceptions aren't swallowed)
        for coroutine in coroutines.keys():
            coroutine.link_exception(thread_error_handler)

        # Wait for all coroutines to terminate
        gevent.wait(coroutines.keys())

        self.log.debug('Finished graph execution')

    def send(self, packet, dest_port):
        q = self._recv_queues[dest_port]
        self.log.debug('Sending packet to %s' % dest_port)
        q.put(packet)

    def receive(self, source_port):
        q = self._recv_queues[source_port]
        component = source_port.component
        while True:
            try:
                packet = q.get(block=False)
                self.log.debug('%s received packet on %s' % (component, source_port))
                component.state = ComponentState.ACTIVE
                return packet
            except queue.Empty:
                if self.is_upstream_terminated(component):
                    # No more data left to receive_packet and upstream has terminated.
                    component.terminate()
                else:
                    self.log.debug('%s is waiting for packet on %s' % (component, source_port))
                    component.suspend()

    def port_has_data(self, port):
        return (port in self._recv_queues and
                not self._recv_queues[port].empty())

    def clear_port(self, port):
        if port in self._recv_queues:
            del self._recv_queues[port]

    def terminate_thread(self):
        raise gevent.GreenletExit

    def suspend_thread(self, seconds=None):
        # Yield control back to the gevent scheduler
        gevent.sleep(seconds)
