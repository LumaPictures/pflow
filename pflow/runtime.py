import logging
import collections
import multiprocessing as mp
from abc import ABCMeta, abstractmethod

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

import gevent
import greenlet
import haigha as amqp

from . import exc
from .graph import Component, ComponentState
from .port import InputPort, OutputPort, \
                  ArrayPort, ArrayInputPort, ArrayOutputPort

log = logging.getLogger(__name__)


# TODO: Implement FBP Network Protocol - http://noflojs.org/documentation/protocol/
# TODO: Scheduling - http://www.jpaulmorrison.com/fbp/schedrls.shtml


class Runtime(object):
    '''
    Schedulers are responsible for starting processes, scheduling execution,
    and forwarding messages on Connections between Processes.
    '''
    __metaclass__ = ABCMeta

    @abstractmethod
    def execute_graph(self, graph):
        '''
        Executes a graph by multitasking all component processes and moving messages along queues.
        '''
        pass

    @abstractmethod
    def send(self, packet, dest_port):
        pass

    @abstractmethod
    def receive(self, source_port):
        pass

    @abstractmethod
    def terminate_thread(self):
        '''
        Terminate this thread.
        It will no longer process packets.
        '''
        pass

    @abstractmethod
    def suspend_thread(self):
        '''
        Suspend execution of this thread until the next packet arrives.
        '''
        pass


class SingleThreadedRuntime(Runtime):
    '''
    Processes are are cooperatively multitasked using gevent and run in a single thread.

    Uses a queue.Queue for Packet buffering.
    Execution is only suspended upon finishing a unit of work and yielding.
    '''
    _gevent_patched = False

    def __init__(self):
        self.gevent_monkey_patch()
        self._recv_queues = collections.defaultdict(queue.Queue)

    def execute_graph(self, graph):
        # Wire up runtime dependency to all components
        for component in graph.components:
            component.runtime = self

        # Find all self-starter components in the graph
        self_starters = graph.self_starters
        if len(self_starters) == 0:
            raise exc.FlowError('Unable to find any self-starter Components in graph')
        else:
            log.info('Self-starter components are: %s' %
                     ', '.join([c.name for c in self_starters]))

        # TODO: Switch from greenlets to gevent so that lower level I/O calls won't block

        def component_runner(component):
            def _run():

                # Activate component
                component.state = ComponentState.ACTIVE

                while not component.is_terminated:

                    # Run the componnet
                    component.run()

                    if component.is_upstream_terminated:
                        # Terminate when all upstream components have terminated and there's no more data to process.
                        component.terminate()
                    else:
                        # Suspend execution until there's more data to process.
                        component.suspend()

            return _run

        def scheduler():
            scheduler_thread = greenlet.getcurrent()  # This thread (scheduler)
            component_threads = {}  # Map of components to their threads
            run_queue = queue.Queue()  # Scheduler running queue

            # Self-starters run first
            for component in self_starters:
                run_queue.put(component)

            try:
                while True:
                    # Get next component to run
                    component = run_queue.get(block=False)
                    #log.info('Run: %s' % component)

                    # Schedule all adjacent downstream components
                    for next_component in component.downstream:
                        run_queue.put(next_component)

                    if component not in graph.components:
                        raise ValueError('Component "%s" was not added to graph' % component.name)

                    # Create thread if it doesn't exist
                    if component not in component_threads:
                        component_threads[component] = greenlet.greenlet(component_runner(component),
                                                                         scheduler_thread)

                    # Context switch to thread
                    component_threads[component].switch()

                    if not component.is_terminated:
                        # Re-schedule
                        run_queue.put(component)

            except queue.Empty:
                log.info('Graph execution has terminated')

        # Start the scheduler
        log.debug('Starting the scheduler...')
        greenlet.greenlet(scheduler).switch()

    def send(self, packet, dest_port):
        q = self._recv_queues[dest_port]
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
                if component.is_upstream_terminated:
                    # No more data left to receive and upstream has terminated.
                    component.terminate()
                else:
                    log.debug('Waiting for packet on %s' % source_port)
                    component.suspend()

    def terminate_thread(self):
        raise greenlet.GreenletExit

    def suspend_thread(self):
        # Switch back to the scheduler greenlet
        greenlet.getcurrent().parent.switch()

    @classmethod
    def gevent_monkey_patch(cls):
        '''
        Monkey patch gevent-affected modules.
        This method is idempotent.
        '''
        import gevent.monkey
        if not cls._gevent_patched:
            log.debug('Monkey patching for gevent...')
            gevent.monkey.patch_all(socket=True,  # socket
                                    dns=True,  # socket dns functions
                                    time=True,  # time.sleep
                                    select=True, # select
                                    aggressive=True,  # select/socket
                                    thread=True,  # thread, threading
                                    os=True,  # os.fork
                                    ssl=True,
                                    httplib=False,
                                    subprocess=True,
                                    sys=True,  # stdin, stdout, stderr
                                    Event=False)
            cls._gevent_patched = True


# class MultiProcessRuntime(Runtime):
#     '''
#     Processes are preemtively multitasked, since each is a multiprocessing.Process.
#
#     Uses either a multiprocessing.Queue or a distributed message queue for Packet buffering.
#     Execution is suspended in one of these cases:
#         1) suspend_thread() is called
#         2) OS kernel preempts another running process.
#     '''
#     pass
