import sys
if 'threading' in sys.modules:
    import gevent.threading
    import threading

    if gevent.threading.Lock != threading.Lock:
        # Crap!
        sys.stderr.write('WARNING: The "threading" module was imported before "%s"; gevent could not monkey '
                         'patch it first, and this may cause graph execution to halt where threads are used!\n' %
                         __name__)

else:
    # Do the monkey patch
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

from .base import GraphRuntime
from ..core import ComponentState
from ..exc import GraphRuntimeError


class SingleProcessGraphRuntime(GraphRuntime):
    """
    Executes a graph in a single process, where each component is run in its own gevent coroutine.

    This low-overhead runtime is useful in environments that are hostile to multiprocessing/threading,
    or when components are mostly I/O bound (i.e. don't need process parallelization).
    """
    def __init__(self, graph):
        super(SingleProcessGraphRuntime, self).__init__(graph)
        self._recv_queues = None

    def execute(self):
        self.log.debug('Executing %s' % self.graph)

        self._recv_queues = collections.defaultdict(queue.Queue)
        coroutines = dict([(gevent.spawn(self.create_component_runner(component),
                                         None,   # in_queues
                                         None),  # out_queues
                            component)
                           for component in self.graph.components])

        def thread_error_handler(coroutine):
            """
            Handles component coroutine exceptions that get raised.
            This should terminate execution of all other coroutines.
            """
            component = coroutines[coroutine]
            self.log.error('Component "%s" failed with %s: %s' % (component.name,
                                                                  coroutine.exception.__class__.__name__,
                                                                  coroutine.exception.message))

            for c in coroutines.values():
                c.terminate(ex=coroutine.exception)

        # Wire up error handler (so that exceptions aren't swallowed)
        for coroutine in coroutines.keys():
            coroutine.link_exception(thread_error_handler)

        # Wait for all coroutines to terminate
        gevent.wait(coroutines.keys())

        self.log.debug('Finished graph execution')

    def send_port(self, component, port_name, packet):
        dest_port = component.outputs[port_name].target_port
        q = self._recv_queues[dest_port.id]
        self.log.debug('Sending packet to %s: %s' % (dest_port, packet))
        component.state = ComponentState.SUSP_SEND
        q.put(packet)
        component.suspend()
        component.state = ComponentState.ACTIVE

    def receive_port(self, component, port_name):
        source_port = component.inputs[port_name]
        q = self._recv_queues[source_port.id]
        component.state = ComponentState.SUSP_RECV
        while True:
            try:
                packet = q.get(block=False)
                self.log.debug('%s received packet on %s: %s' % (component, source_port, packet))
                component.state = ComponentState.ACTIVE
                return packet
            except queue.Empty:
                if self.graph.is_upstream_terminated(component):
                    # No more data left to receive_packet and upstream has terminated.
                    self.log.debug('%s will be terminated because of dead upstream (receive_port: %s)' %
                                   (component, source_port))
                    component.state = ComponentState.ACTIVE
                    component.terminate()
                else:
                    #self.log.debug('%s is waiting for packet on %s' % (component, source_port_id))
                    component.suspend()

    def close_input_port(self, component, port_name):
        self.log.debug('Closing input port %s.%s' % (component.name, port_name))

        port_id = component.inputs[port_name].id
        if port_id in self._recv_queues:
            del self._recv_queues[port_id]

    def close_output_port(self, component, port_name):
        self.log.debug('Closing output port %s.%s' % (component.name, port_name))

        port_id = component.outputs[port_name].id
        if port_id in self._recv_queues:
            del self._recv_queues[port_id]

    def terminate_thread(self, component):
        raise gevent.GreenletExit

    def suspend_thread(self, seconds=None):
        # Yield control back to the gevent scheduler
        gevent.sleep(seconds)
