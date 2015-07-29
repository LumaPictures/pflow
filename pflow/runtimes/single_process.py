import sys
if 'threading' in sys.modules:
    raise Exception('threading module loaded before gevent monkey patching!')

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

import logging
import collections

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

import gevent

from .base import Runtime
from .. import exc
from ..core import ComponentState

log = logging.getLogger(__name__)


class SingleThreadedRuntime(Runtime):
    '''
    Processes are are cooperatively multitasked using gevent and run in a single thread.

    Uses a queue.Queue for Packet buffering.
    Execution is only suspended upon finishing a unit of work and yielding.
    '''
    def __init__(self):
        self._recv_queues = collections.defaultdict(queue.Queue)

    def execute_graph(self, graph):
        self._inject_runtime(graph)

        # Find all self-starter components in the graph
        self_starters = graph.self_starters
        if len(self_starters) == 0:
            log.warn('%s is a no-op graph because there are no self-starter components' % graph.name)
            # raise exc.FlowError('Unable to find any self-starter Components in graph')
        else:
            log.debug('Self-starter components are: %s' %
                      ', '.join([c.name for c in self_starters]))

        def component_runner(component):
            def component_loop():
                while not component.is_terminated:

                    # Activate component
                    component.state = ComponentState.ACTIVE

                    # Run the component
                    component.run()

                    if self.is_upstream_terminated(component):
                        # Terminate when all upstream components have terminated and there's no more data to process.
                        component.terminate()
                    else:
                        # Suspend execution until there's more data to process.
                        component.suspend()

                    # TODO: Detect condition where all inputs would never be satisfied
                    # (e.g. an upstream component to a binary operator died)

            return component_loop

        component_threads = dict([(gevent.spawn(component_runner(c)), c)
                                  for c in graph.components])

        def thread_error_handler(thread):
            '''
            Handles component thread exceptions that get raised.
            This should terminate execution of all other threads and re-raise the error.
            '''
            component = component_threads[thread]
            log.error('Component "%s" failed with %s: %s' % (component.name,
                                                             thread.exception.__class__.__name__,
                                                             thread.exception.message))

            gevent.killall(component_threads.keys())

        # Wire up error handler (so that exceptions aren't swallowed)
        for thread in component_threads.keys():
            thread.link_exception(thread_error_handler)

        gevent.wait(component_threads.keys())

    def send(self, packet, dest_port):
        q = self._recv_queues[dest_port]
        log.debug('Sending packet to %s' % dest_port)
        q.put(packet)

    def receive(self, source_port):
        q = self._recv_queues[source_port]
        while True:
            try:
                packet = q.get(block=False)
                log.debug('Received packet on %s' % source_port)
                return packet
            except queue.Empty:
                component = source_port.component
                if self.is_upstream_terminated(component):
                    # No more data left to receive_packet and upstream has terminated.
                    component.terminate()
                else:
                    log.debug('Waiting for packet on %s' % source_port)
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
        # Yield control back to the scheduler
        gevent.sleep(seconds)
