from abc import ABCMeta, abstractmethod


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
    def port_has_data(self, port):
        pass

    @abstractmethod
    def clear_port(self, port):
        pass

    @abstractmethod
    def terminate_thread(self):
        '''
        Terminate this thread.
        It will no longer process packets.
        '''
        pass

    @abstractmethod
    def suspend_thread(self, seconds=None):
        '''
        Suspend execution of this thread until the next packet arrives.
        '''
        pass


class RuntimeTarget(object):
    '''
    Class that can have a Runtime injected into it after graph construction.
    Runtimes implement scheduling behavior.
    '''
    __metaclass__ = ABCMeta

    @property
    def runtime(self):
        if not hasattr(self, '_runtime'):
            raise ValueError('You need to run this graph through a Runtime.')

        return self._runtime

    @runtime.setter
    def runtime(self, runtime):
        if hasattr(self, '_runtime') and runtime != self._runtime:
            raise ValueError('Runtime can not be changed. Please re-create the graph.')

        self._runtime = runtime
