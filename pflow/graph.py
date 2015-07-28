from abc import ABCMeta, abstractmethod
import logging

try:
    from queue import Queue  # 3.x
except ImportError:
    from Queue import Queue  # 2.x
from enum import Enum  # 3.x (or enum34 backport

from . import exc
from .port import PortRegistry, InputPort, OutputPort, ArrayInputPort, ArrayOutputPort

log = logging.getLogger(__name__)


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


class Packet(object):
    '''
    Information packet (IP)
    '''
    def __init__(self, value):
        self.value = value
        self._owner = None  # Component that owns this
        self.attrs = {}  # Named attributes

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        # TODO: unset old owner by decrementing self._owner.owned_packet_count
        self._owner = value

    def __str__(self):
        return 'Packet(%s)' % self.value


class BracketPacket(Packet):
    '''
    Special packet used for bracketing.
    '''
    __metaclass__ = ABCMeta


class StartBracket(BracketPacket):
    '''
    Start of bracketed data.
    '''
    pass


class EndBracket(BracketPacket):
    '''
    End of bracketed data.
    '''
    pass


class ComponentState(Enum):

    # Initial component state before it receives first packet.
    NOT_STARTED = 'NOT_STARTED'

    # Component should still be ACTIVEly receiving data.
    ACTIVE = 'ACTIVE'

    # Component processed last data item and is SUSPENDED.
    # until the next packet arrives.
    SUSPENDED = 'SUSPENDED'

    # Component has either TERMINATED itself or has processed
    # the last packet. This component is automatically considered
    # TERMINATED it: 1) has no more data to process, and 2) all its
    # upstream components are TERMINATED.
    TERMINATED = 'TERMINATED'

    # Component has been TERMINATED with an ERROR status. This can
    # happen when an unexpected exception is raised.
    ERROR = 'ERROR'


class Component(RuntimeTarget):
    '''
    Component instances are "process" nodes in a flow-based digraph.

    Each Component has zero or more input and output ports, which are
    connected to other Components through Ports.
    '''
    __metaclass__ = ABCMeta

    # Valid state transitions for components
    _valid_transitions = (
        (ComponentState.NOT_STARTED, ComponentState.ACTIVE),

        (ComponentState.ACTIVE, ComponentState.SUSPENDED),
        (ComponentState.ACTIVE, ComponentState.TERMINATED),
        (ComponentState.ACTIVE, ComponentState.ERROR),

        (ComponentState.SUSPENDED, ComponentState.ACTIVE),
        (ComponentState.SUSPENDED, ComponentState.TERMINATED),
        (ComponentState.SUSPENDED, ComponentState.ERROR)
    )

    def __init__(self, name):
        if not isinstance(name, basestring):
            raise ValueError('name must be a string')

        self.name = name
        self.inputs = PortRegistry(self, InputPort, ArrayInputPort)
        self.outputs = PortRegistry(self, OutputPort, ArrayOutputPort)
        self._state = ComponentState.NOT_STARTED
        self.owned_packet_count = 0
        self.initialize()

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        old_state = self._state

        # Ensure state transition is a valid one.
        if (old_state, new_state) not in self._valid_transitions:
            raise exc.ComponentStateError('Invalid state transition for component "%s": %s -> %s' %
                                          (self.name, old_state.value, new_state.value))

        self._state = new_state

        log.debug('Component "%s" transitioned from %s -> %s' %
                  (self.name, old_state.value, new_state.value))

        # TODO: Fire a transition event

    @property
    def upstream(self):
        '''
        Immediate upstream components.
        '''
        upstream = set()

        for port in self.inputs:
            if port.is_connected:
                upstream.add(port.source_port.component)

        return upstream

    @property
    def downstream(self):
        '''
        Immediate downstream components.
        '''
        downstream = set()

        for port in self.outputs:
            if port.is_connected:
                downstream.add(port.target_port.component)

        return downstream

    @abstractmethod
    def initialize(self):
        '''
        Initialization code necessary to define this component.
        '''
        pass

    @abstractmethod
    def run(self):
        '''
        This method is called any time the port is open and a new Packet arrives.
        '''
        pass

    def create_packet(self, value=None):
        packet = Packet(value)
        packet.owner = self
        self.owned_packet_count += 1
        return packet

    def drop(self, packet):
        '''
        Drop a Packet.
        '''
        raise NotImplementedError

    def validate(self):
        '''
        Validate component state and the state of all its ports.
        Raises a FlowError if there was a problem.
        '''
        # raise exc.FlowError, 'This component is invalid!'
        log.debug('Validating component "%s"...' % self.name)

        # TODO: Ensure there's at least 1 port defined
        # TODO: Ensure all ports are connected
        # TODO: Ensure all non-optional ports have connections

        return self

    @property
    def is_upstream_terminated(self):
        return all([c.is_terminated for c in self.upstream])

    @property
    def is_terminated(self):
        '''
        Has this component been terminated?
        '''
        return self.state in (ComponentState.TERMINATED,
                              ComponentState.ERROR)

    def terminate(self, ex=None):
        '''
        Terminate execution for this component.
        This will not terminate upstream components!
        '''
        if self.is_terminated:
            raise ValueError('Component "%s" is already terminated' % self.name)

        if ex is not None:
            log.error('Component "%s" is abnormally terminating from %s...' % (self.name,
                                                                               ex.__class__.__name__))
            self.state = ComponentState.ERROR

            # Close all input ports so signal upstream components
            for input in self.inputs:
                if input.is_open:
                    input.close()
        else:
            self.state = ComponentState.TERMINATED

        self.runtime.terminate_thread()

    def suspend(self):
        '''
        Yield execution to scheduler.
        '''
        self.state = ComponentState.SUSPENDED
        self.runtime.suspend_thread()
        if not self.is_terminated:
            self.state = ComponentState.ACTIVE

    def __str__(self):
        return ('Component(%s, inputs=%s, outputs=%s)' %
                (self.name, self.inputs, self.outputs))


class InitialPacketGenerator(Component):
    '''
    An initial packet (IIP) generator that is connected to an input port
    of a component.

    This should have no input ports, a single output port, and is used to
    make the logic easier.
    '''
    def __init__(self, value):
        import uuid

        self.value = value
        super(InitialPacketGenerator, self).__init__('IIP_GEN_%s' % uuid.uuid4().hex)

    def initialize(self):
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        iip = self.create_packet(self.value)
        log.debug('IIP: %s' % iip)
        self.outputs['OUT'].send(iip)


class Graph(Component):
    '''
    Execution graph.

    This can also be used as a subgraph for composite components.
    '''
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        self.components = set()  # Nodes
        super(Graph, self).__init__(*args, **kwargs)

    def add_component(self, *components):
        for component in components:
            self.components.add(component)

        return self

    @property
    def self_starters(self):
        '''
        Returns a set of all self-starter components.
        '''
        # Has only disconnected optional inputs
        def is_self_starter_input(input_port):
            return (input_port.optional and
                    input_port.source_port is None)

        self_starters = set()
        for node in self.components:
            # Self-starter nodes should have either no inputs or only have disconnected optional inputs
            if len(node.inputs) == 0:
                # No inputs
                self_starters.add(node)
            elif all(map(is_self_starter_input, node.inputs)):
                self_starters.add(node)

        return self_starters

    def run(self):
        self.validate()
        log.debug('Executing graph...')

        # TODO: find and run all self-starters

        pass

    @property
    def is_terminated(self):
        '''
        Has this graph been terminated?
        '''
        return all([component.is_terminated for component in self.components])

    def validate(self):
        log.debug('Validating graph...')

        # TODO: ensure graph contains no components that aren't in self.components
        # TODO: validate all components
        # TODO: ensure all components are connected
        # TODO: ensure all edges have components
        # TODO: warn if there's potential deadlocks

        return self

    def load_fbp_string(self, fbp_script):
        # TODO: find python fbp parser
        # TODO: parse fbp string and build graph

        return self

    def load_fbp_file(self, file_path):
        with open(file_path, 'r') as f:
            self.load_fbp_string(f.read())

        return self

    def write_graphml(self, file_path):
        '''
        Writes this Graph as a *.graphml file.

        This is useful for debugging network configurations that are hard
        to visualize purely with code.

        :param file_path: the file to write to (should have a .graphml extension)
        '''
        import networkx as nx

        graph = nx.DiGraph()

        def _build_nodes(components):
            visited_nodes = set()

            for component in components:
                if component not in visited_nodes:
                    visited_nodes.add(component)
                    graph.add_node(component.name)

                node_attribs = {
                    'label': '%s\n(%s)' % (component.name,
                                           component.__class__.__name__),
                    'description': (component.__class__.__doc__ or '')
                }

                graph.node[component.name].update(node_attribs)

        def _build_edges(components, visited_nodes=None):
            if visited_nodes is None:
                visited_nodes = set()

            if len(components) > 0:
                next_components = set()

                for component in components:
                    if component in visited_nodes:
                        continue

                    for output in component.outputs:
                        next_components.add(output.target_port.component)

                        edge_attribs = {
                            'label': '%s -> %s' % (output.name,
                                                   output.target_port.name),
                            'description': (output.description or '')
                        }

                        graph.add_edge(component.name,
                                       output.target_port.component.name,
                                       edge_attribs)

                    visited_nodes.add(component)

                _build_edges(next_components, visited_nodes)

        log.debug('Building nx graph...')
        _build_nodes(self.components)
        _build_edges(self.components)

        log.debug('Writing graph to "%s"...' % file_path)
        nx.write_graphml(graph, file_path)
