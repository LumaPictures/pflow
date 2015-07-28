import logging
import collections
import multiprocessing as mp
from abc import ABCMeta, abstractmethod

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

import gevent
#import greenlet
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

    def _inject_runtime(self, graph):
        # Wire up runtime dependency to all components
        for component in graph.components:
            component.runtime = self

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
        self._inject_runtime(graph)

        # Find all self-starter components in the graph
        self_starters = graph.self_starters
        if len(self_starters) == 0:
            raise exc.FlowError('Unable to find any self-starter Components in graph')
        else:
            log.info('Self-starter components are: %s' %
                     ', '.join([c.name for c in self_starters]))

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

        component_threads = [gevent.spawn(component_runner(c)) for c in graph.components]
        gevent.wait(component_threads)

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
        raise gevent.GreenletExit

    def suspend_thread(self, seconds=None):
        # Yield control back to the scheduler
        gevent.sleep(seconds)

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
                                    sys=False,  # stdin, stdout, stderr
                                    Event=False)
            cls._gevent_patched = True


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

        # Find all self-starter components in the graph
        self_starters = graph.self_starters
        if len(self_starters) == 0:
            raise exc.FlowError('Unable to find any self-starter Components in graph')
        else:
            log.info('Self-starter components are: %s' %
                     ', '.join([c.name for c in self_starters]))

        raise NotImplementedError

    def send(self, packet, dest_port):
        raise NotImplementedError

    def receive(self, source_port):
        raise NotImplementedError

    def terminate_thread(self):
        raise NotImplementedError

    def suspend_thread(self, seconds=None):
        raise NotImplementedError
