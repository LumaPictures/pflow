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
