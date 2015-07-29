from abc import ABCMeta, abstractmethod


class Runtime(object):
    '''
    Schedulers are responsible for starting processes, scheduling execution,
    and forwarding messages on Connections between Processes.
    '''
    __metaclass__ = ABCMeta

    def __init__(self):
        import logging
        self.log = logging.getLogger(self.__class__.__name__)

    def inject_runtime(self, graph):
        '''
        Wire up runtime dependency to all components in the graph.
        '''
        for component in graph.components:
            component.runtime = self

    def get_self_starters(self, graph):
        '''
        Gets all self-starter components in the graph.
        '''
        self_starters = graph.self_starters
        if len(self_starters) == 0:
            self.log.warn('%s is a no-op graph because there are no self-starter components' % graph.name)
            # raise exc.FlowError('Unable to find any self-starter Components in graph')
        else:
            self.log.debug('Self-starter components are: %s' %
                           ', '.join([c.name for c in self_starters]))

        return self_starters

    def is_upstream_terminated(self, component):
        dead_parents = all([c.is_terminated for c in component.upstream])
        inputs_have_data = any([self.port_has_data(p) for p in component.inputs])
        return dead_parents and not inputs_have_data

    def create_component_runner(self, component):
        from ..core import ComponentState

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
