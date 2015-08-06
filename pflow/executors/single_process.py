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

import time
import collections

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

import gevent

from .base import GraphExecutor
from ..core import ComponentState
from ..port import Port, InputPort, EndOfStream
from .. import exc


class SingleProcessGraphExecutor(GraphExecutor):
    """
    Executes a graph in a single process, where each component is run in its own gevent coroutine.

    This low-overhead runtime is useful in environments that are hostile to multiprocessing/threading,
    or when components are mostly I/O bound (i.e. don't need process parallelization).
    """
    def __init__(self, graph):
        super(SingleProcessGraphExecutor, self).__init__(graph)
        self._recv_queues = None
        self._running = False
        self._coroutines = None

    def execute(self):
        self._running = True
        self.log.debug('Executing %s' % self.graph)

        self._recv_queues = collections.defaultdict(queue.Queue)
        self._coroutines = dict([(gevent.spawn(self._create_component_runner(component),
                                               None,   # in_queues
                                               None),  # out_queues
                                  component)
                                 for component in self.graph.components])

        def thread_error_handler(coroutine):
            """
            Handles component coroutine exceptions that get raised.
            This should terminate execution of all other coroutines.
            """
            component = self._coroutines[coroutine]
            self.log.error('Component "%s" failed with %s: %s' % (component.name,
                                                                  coroutine.exception.__class__.__name__,
                                                                  coroutine.exception.message))

            for c in self._coroutines.values():
                if not c.is_terminated:
                    c.terminate(ex=coroutine.exception)

        # Wire up error handler (so that exceptions aren't swallowed)
        for coroutine in self._coroutines.keys():
            coroutine.link_exception(thread_error_handler)

        # Wait for all coroutines to terminate
        gevent.wait(self._coroutines.keys())

        self._reset_components()
        self._running = False
        self.log.debug('Finished graph execution')

    def is_running(self):
        return self._running

    def _get_or_create_queue(self, port):
        if not isinstance(port, Port):
            raise ValueError('port must be a Port')

        if port.id not in self._recv_queues:
            if isinstance(port, InputPort):
                maxsize = port.max_queue_size
            else:
                maxsize = None

            self._recv_queues[port.id] = queue.Queue(maxsize=maxsize)

        return self._recv_queues[port.id]

    def send_port(self, component, port_name, packet, timeout=None):
        source_port = component.outputs[port_name]
        dest_port = source_port.target_port
        q = self._get_or_create_queue(dest_port)

        component.state = ComponentState.SUSP_SEND

        self.log.debug('Sending packet from %s to %s: %s' %
                       (source_port, dest_port, packet))

        try:
            # TODO: Make this call non-blocking
            q.put(packet)
            component.suspend()
            component.state = ComponentState.ACTIVE
        except queue.Full:
            # Timed out
            component.state = ComponentState.ACTIVE
            raise exc.PortTimeout

    def receive_port(self, component, port_name, timeout=None):
        source_port = component.inputs[port_name]
        if not source_port.is_open():
            return EndOfStream

        q = self._get_or_create_queue(source_port)
        component.state = ComponentState.SUSP_RECV

        self.log.debug('%s is waiting for data on %s' % (component, source_port))
        start_time = time.time()
        while not component.is_terminated:
            try:
                packet = q.get(block=False)
                self.log.debug('%s received packet on %s: %s' % (component, source_port, packet))
                component.state = ComponentState.ACTIVE
                return packet
            except queue.Empty:
                curr_time = time.time()
                if timeout is not None and curr_time - start_time >= timeout:
                    component.state = ComponentState.ACTIVE
                    raise exc.PortTimeout

                if self.graph.is_upstream_terminated(component):
                    # No more data left to receive_packet and upstream has terminated.
                    component.state = ComponentState.ACTIVE

                    if source_port.is_open():
                        source_port.close()

                    return EndOfStream
                else:
                    # self.log.debug('%s is waiting for packet on %s' % (component, source_port))
                    component.state = ComponentState.SUSP_RECV
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
