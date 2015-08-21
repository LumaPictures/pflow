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
import greenlet

from .base import GraphExecutor
from ..core import ComponentState
from ..port import Port, InputPort, EndOfStream
from .. import exc


class SingleProcessGraphExecutor(GraphExecutor):
    """
    Executes a graph in a single process, where each component is run in its
    own gevent coroutine.

    This low-overhead runtime is useful in environments that are hostile to
    multiprocessing/threading, or when components are mostly I/O bound (i.e.
    don't need process parallelization).
    """
    DETECT_BLOCKING = True   # Should blocking greenlets be detected?
    MAX_BLOCKING_TIME = 1.0  # Max number of seconds a greenlet can block before a warning is logged

    def __init__(self, graph):
        super(SingleProcessGraphExecutor, self).__init__(graph)
        self._recv_queues = None
        self._running = False
        self._coroutines = None
        self._last_switch_time = None

    def _blocking_greenlet_detector(self, event, (origin, target)):
        """
        Greenlet tracer that detects greenlets that block for too long.

        These are CPU-bound or are making a non-monkeypatched synchronous call,
        which can cause graph execution to slow down or deadlock. It's not possible
        to detect deadlocks with this trace function, however.
        """
        if event != 'switch':
            return  # Only care about context switches and not 'throw's

        then = self._last_switch_time
        now = self._last_switch_time = time.time()
        if then is None:
            return

        blocking_time = now - then
        if origin is not gevent.get_hub() and blocking_time > self.MAX_BLOCKING_TIME:
            component = self._coroutines.get(origin)
            if component is None:
                return  # Non-component greenlet

            self.log.warn('{} blocked the event loop for {:.2f} seconds!\n'
                          'If the component was waiting on a blocking call, be sure to gevent.monkey patch it '
                          'or convert it to an asynchronous call. CPU-bound components aren\'t well suited for '
                          '{}, and should use a different executor altogether.'.format(component,
                                                                                       blocking_time,
                                                                                       self.__class__.__name__))
            #traceback.print_stack()

    def execute(self):
        self.log.debug('Executing {}'.format(self.graph))

        # Initialize graph
        if self.graph.state == ComponentState.NOT_INITIALIZED:
            self.graph.initialize()
            self.graph.state = ComponentState.INITIALIZED

        # Enable tracing
        old_trace = greenlet.gettrace()
        if self.DETECT_BLOCKING:
            greenlet.settrace(self._blocking_greenlet_detector)

        self._running = True
        try:
            all_components = self.graph.get_all_components()
            for component in self.graph.get_all_components(include_graphs=True):
                component.executor = self

            self.log.debug('Components in {}: {}'.format(self.graph,
                                                         ', '.join(map(str, all_components))))

            self._recv_queues = collections.defaultdict(queue.Queue)
            self._coroutines = dict(
                [(gevent.spawn(self._create_component_runner(comp),
                               None,   # in_queues
                               None),  # out_queues
                  comp) for comp in all_components]
            )

            last_exception = None

            def thread_error_handler(coroutine):
                """
                Handles component coroutine exceptions that get raised.
                This should terminate execution of all other coroutines.
                """
                last_exception = coroutine.exception

                component = self._coroutines[coroutine]
                self.log.error('Component "{}" failed with {}: {}'.format(
                    component.name, coroutine.exception.__class__.__name__,
                    coroutine.exception.message))

                for c in self._coroutines.values():
                    if c.is_alive():
                        c.terminate(ex=last_exception)

            # Wire up error handler (so that exceptions aren't swallowed)
            for coroutine in self._coroutines.keys():
                coroutine.link_exception(thread_error_handler)

            # Wait for all coroutines to terminate
            gevent.wait(self._coroutines.keys())

            self.graph.terminate(ex=last_exception)
            self._final_checks()
            self._reset_components()

            self.log.debug('Finished graph execution')

        finally:
            self._running = False
            if self.DETECT_BLOCKING:
                # Unset tracer
                greenlet.settrace(old_trace)

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

        self.log.debug('Sending packet from {} to {}: {}'.format(
                       source_port, dest_port, packet))

        try:
            # TODO: Make this call non-blocking
            q.put(packet)
            self.suspend_thread()
            component.state = ComponentState.ACTIVE
        except queue.Full:
            # Timed out
            component.state = ComponentState.ACTIVE
            raise exc.PortTimeout(dest_port)

    def receive_port(self, component, port_name, timeout=None):
        source_port = component.inputs[port_name]
        if not source_port.is_open():
            return EndOfStream

        q = self._get_or_create_queue(source_port)
        component.state = ComponentState.SUSP_RECV

        self.log.debug('{} is waiting for data on {}'.format(component,
                                                             source_port))
        start_time = time.time()
        while component.is_alive():
            try:
                packet = q.get(block=False)
                self.log.debug('{} received packet on {}: {}'.format(
                    component, source_port, packet))
                component.state = ComponentState.ACTIVE
                return packet
            except queue.Empty:
                curr_time = time.time()
                if timeout is not None and curr_time - start_time >= timeout:
                    component.state = ComponentState.ACTIVE
                    raise exc.PortTimeout(source_port)

                if self.graph.is_upstream_terminated(component):
                    # No more data left to receive_packet and upstream has
                    # terminated.
                    component.state = ComponentState.ACTIVE

                    if source_port.is_open():
                        source_port.close()

                    return EndOfStream
                else:
                    # self.log.debug('%s is waiting for packet on %s' % (component, source_port))
                    component.state = ComponentState.SUSP_RECV
                    self.suspend_thread()

    def close_input_port(self, component, port_name):
        self.log.debug('Closing input port {}.{}'.format(component.name,
                                                         port_name))

        port_id = component.inputs[port_name].id
        if port_id in self._recv_queues:
            del self._recv_queues[port_id]

    def close_output_port(self, component, port_name):
        self.log.debug('Closing output port {}.{}'.format(component.name,
                                                          port_name))

        port_id = component.outputs[port_name].id
        if port_id in self._recv_queues:
            del self._recv_queues[port_id]

    def terminate_thread(self, component):
        raise gevent.GreenletExit

    def suspend_thread(self, seconds=None):
        if seconds is None or seconds <= 0:
            # gevent fix, since anything <= 0 can cause other greenlets not to
            # get scheduled. Here's the description from their recent v1.1
            # gevent.hub.sleep docstring:
            #
            # In the current implementation, a value of 0 (the default)
            # means to yield execution to any other runnable greenlets, but
            # this greenlet may be scheduled again before the event loop
            # cycles (in an extreme case, a greenlet that repeatedly sleeps
            # with 0 can prevent greenlets that are ready to do I/O from
            # being scheduled for some (small) period of time); a value greater
            # than 0, on the other hand, will delay running this greenlet until
            # the next iteration of the loop.
            seconds = 0.1

        # Yield control back to the gevent scheduler
        gevent.sleep(seconds)
