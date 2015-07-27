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
                component.state = ComponentState.ACTIVE

                while not component.is_terminated:
                    component.run()
                    if all([c.is_terminated for c in component.upstream]):
                        component.terminate()
                    else:
                        component.suspend()

            return _run

        def scheduler():
            scheduler_thread = greenlet.getcurrent()
            component_threads = dict([(component, greenlet.greenlet(component_runner(component), scheduler_thread))
                                        for component in graph.components])

            # First run all self-starters...
            log.debug('Running all self-starter components...')
            for component in self_starters:
                #log.debug('Switch: %s' % component.name)
                component_threads[component].switch()

                # TODO
                for output in component.outputs:
                    if isinstance(output, ArrayPort):
                        raise NotImplementedError('ArrayPort not implemented yet')

            # ...then run the rest of the graph
            log.debug('Running the rest of the graph...')
            active_components = set(graph.components)
            while len(active_components) > 0:
                for component in set(active_components):
                    if component.is_terminated:
                        # Deactivate terminated component
                        log.debug('Component "%s" has been terminated' % component.name)
                        active_components.remove(component)
                    else:
                        # Context switch
                        # TODO: Determine if component has pending input data
                        #log.debug('Switch: %s' % component.name)
                        component_threads[component].switch()

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
                log.debug('Suspending receive on %s' % source_port)
                self.suspend_thread()

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
