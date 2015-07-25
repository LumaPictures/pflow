import logging
import multiprocessing as mp
from abc import ABCMeta, abstractmethod

try:
    from queue import Queue  # 3.x
except ImportError:
    from Queue import Queue  # 2.x

import gevent
import haigha as amqp

from . import exc
from .graph import Component, \
    InputPort, OutputPort, \
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
        Initialize a graph into an instance and execute it.
        '''
        pass

    @abstractmethod
    def send(self, packet, dest_port):
        pass

    @abstractmethod
    def receive(self, source_port):
        pass

    @abstractmethod
    def yield_control(self):
        '''
        Yield this thread's control to the scheduler.
        '''
        pass


class GeventRuntime(Runtime):
    '''
    Processes are are cooperatively multitasked using gevent run in a single thread.

    Uses a queue.Queue for Packet buffering.
    Execution is only suspended upon finishing a unit of work and yielding.
    '''
    _gevent_patched = False
    _port_buffers = {}  # Buffers for edges, keyed by (source_port, dest_port) tuples.

    def __init__(self):
        self.gevent_monkey_patch()

    def execute_graph(self, graph):
        self_starters = graph.self_starters
        if len(self_starters) == 0:
            raise exc.FlowError('Unable to find any self-starter Components in graph')

        log.info('Self-starter components are: %s' % self_starters)

        for component in self_starters:
            component._run()
            for output in component.outputs:
                if isinstance(output, ArrayPort):
                    raise NotImplementedError('ArrayPort not implemented yet')

    def send(self, packet, dest_port):
        raise NotImplementedError

    def receive(self, source_port):
        raise NotImplementedError

    def yield_control(self):
        gevent.sleep()

    @classmethod
    def gevent_monkey_patch(cls):
        '''
        Monkey patch gevent-affected modules.
        This method is idempotent.
        '''
        import gevent.monkey
        if not cls._gevent_patched:
            log.debug('Monkey patching for gevent...')
            gevent.monkey.patch_socket()  # socket
            gevent.monkey.patch_time()  # time.sleep
            cls._gevent_patched = True


# class MultiProcessRuntime(Runtime):
#     '''
#     Processes are preemtively multitasked, since each is a multiprocessing.Process.
#
#     Uses either a multiprocessing.Queue or a distributed message queue for Packet buffering.
#     Execution is suspended in one of these cases:
#         1) Yielded to scheduler after finishing a unit of work.
#         2) OS kernel preempts another running process.
#     '''
#     pass
