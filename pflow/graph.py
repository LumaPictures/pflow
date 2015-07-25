from abc import ABCMeta, abstractmethod
import logging

try:
    from queue import Queue  # 3.x
except ImportError:
    from Queue import Queue  # 2.x
from enum import Enum  # 3.x (or enum34 backport

from . import exc

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
            raise ValueError('runtime not initialized yet')

        return self._runtime

    @runtime.setter
    def runtime(self, runtime):
        if hasattr(self, '_runtime') and runtime != self._runtime:
            raise ValueError('runtime can not be changed after being set')

        self._runtime = runtime


class BasePort(object):
    __metaclass__ = ABCMeta


class Port(BasePort):
    __metaclass__ = ABCMeta

    def __init__(self, name, description=None, optional=False, type_=str, auto_open=True):
        if not isinstance(name, basestring):
            raise ValueError('name must be a string')

        self.name = name
        self.description = description
        self.optional = optional  # Is this port optional?
        self.type_ = type_  # Data type
        self.auto_open = auto_open  # Is this port auto-opening?

        self.component = None  # Owning component
        self.source_port = None
        self.target_port = None
        self._buffer = Queue()  # TODO: Delegate to runtime
        self._is_open = False

    def open(self):
        if not self.is_connected:
            raise exc.PortError('Port "%s" is disconnected (%s, %s, %s), and can not be opened' %
                                (self._port_name, self.component, self.target_port, self.source_port))

        if self._is_open:
            raise exc.PortError('Port "%s" is already open' % self._port_name)

        self._is_open = True
        # TODO

    def close(self):
        if self._is_open:
            raise exc.PortError('Port "%s" is already closed' % self._port_name)

        self._is_open = False

    @property
    def is_open(self):
        return self.is_connected and self._is_open

    def connect(self, target_port):
        '''
        Connect this Port to an InputPort
        '''
        if not isinstance(target_port, Port):
            raise ValueError('target_port must be a Port')

        if target_port.source_port is not None:
            raise exc.PortError('target_port is already connected to another source')

        self.target_port = target_port
        target_port.source_port = self

        log.debug('Port "%s.%s" connected to "%s.%s"' %
                  (self.component.name, self.name,
                   target_port.component.name, target_port.name))

        if self.auto_open:
            log.debug('Opening Port "%s" (auto)' % self._port_name)
            self.open()

    @property
    def is_connected(self):
        return (self.component is not None and
                self.target_port is not None)
                #self.source_port is not None)

    @property
    def _port_name(self):
        if self.component is not None:
            component_name = self.component.name
        else:
            component_name = '(no_component)'

        return '%s.%s' % (component_name, self.name)

    def _check_ready_state(self):
        if not self.is_open:
            raise exc.PortClosedError('Port "%s" is closed' % self._port_name)

    def __getitem__(self, index):
        raise ValueError('Port "%s" is not an array port' % self._port_name)

    def __iter__(self):
        raise ValueError('Port "%s" is not an array port' % self._port_name)

    def __str__(self):
        return 'Port(%s%s)' % (self._port_name,
                               '*' if self.optional else '')


class InputPort(Port):
    '''
    Port that is defined on the input side of a component.
    Leads from either an OutputPort of an upstream component or an initial Packet.
    '''
    def __init__(self, name='IN', **kwargs):
        super(InputPort, self).__init__(name, **kwargs)

    def receive(self):
        '''
        Receive the next Packet from this input port.

        :return: Packet that was received or None if EOF
        '''
        # Optional port with no connection
        if self.optional and not self.is_connected:
            return None
        else:
            self._check_ready_state()

        # TODO: yield execution

        # TODO: claim ownership
        # TODO: increment refcount

        log.debug('Receiving packet over port %s' % self.name)

        #raise NotImplementedError


class ArrayPort(BasePort):
    __metaclass__ = ABCMeta

    def __init__(self, name, max_ports, **kwargs):
        if not isinstance(name, basestring):
            raise ValueError('name must be a string')

        self._name = name
        self._max_ports = max_ports
        self._kwargs = kwargs

        self._ports = []
        self.allocate()

    def allocate(self):
        '''
        Allocate array port.
        '''
        self._ports = []
        port_class = self.get_port_class()
        for i in range(self._max_ports):
            self._ports.append(port_class('%s_%d' % (self._name, i),
                                          **self._kwargs))

    @abstractmethod
    def get_port_class(self):
        return Port  # Make IDE shut up

    def __getitem__(self, index):
        return self._ports[index]

    def __iter__(self):
        return iter(self._ports)


class ArrayInputPort(ArrayPort):
    @abstractmethod
    def get_port_class(self):
        return InputPort


class OutputPort(Port):
    '''
    Port that is defined on the output side of a Component.
    Leads into an InputPort of a downstream component.
    '''
    def __init__(self, name='OUT', **kwargs):
        super(OutputPort, self).__init__(name, **kwargs)

    def send(self, packet):
        '''
        Send a single Packet over this output port.

        :param packet: the Packet to send over this output port.
        '''
        self._check_ready_state()

        # TODO: decrement refcount

        log.debug('Sending packet over port %s' % self.name)

        # TODO: yield execution


class ArrayOutputPort(ArrayPort):
    @abstractmethod
    def get_port_class(self):
        return InputPort


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


class ComponentPorts(object):
    '''
    Descriptor that allows addressing ports by name (as attributes).
    '''
    def __init__(self, component, *required_superclasses):
        self._ports = {}
        self._component = component

        if not all(map(lambda c: issubclass(c, BasePort), required_superclasses)):
            raise ValueError('required_superclass must be Port subclass')

        self._required_superclasses = required_superclasses

    def add(self, port):
        if not isinstance(port, self._required_superclasses):
            raise ValueError('Port "%s" must be an instance of: %s' %
                             (port.name, ', '.join([c.__name__ for c in self._required_superclasses])))

        if port.name in self._ports:
            raise ValueError('Port "%s" already exists' % port.name)

        if port.component is not None and port.component != self._component:
            raise ValueError('Port "%s" is already attached to Component "%s"' %
                             (port.name, port.component.name))

        port.component = self._component
        self._ports[port.name] = port

        return self

    def __get__(self, port_name):
        return self[port_name]

    def __getitem__(self, port_name):
        if port_name not in self._ports:
            raise AttributeError('Component does not have a port named "%s"' %
                                 port_name)

        return self._ports[port_name]

    def __iter__(self):
        return iter(self._ports.values())

    def __len__(self):
        return len(self._ports)

    def __str__(self):
        return '(%s)' % ', '.join(map(str, self._ports.values()))


class ComponentState(Enum):
    NOT_STARTED = 1
    ACTIVE = 2
    DORMANT = 3
    TERMINATED = 4
    ERROR = 5


class Component(RuntimeTarget):
    __metaclass__ = ABCMeta

    def __init__(self, name):
        if not isinstance(name, basestring):
            raise ValueError('name must be a string')

        self.name = name
        self.inputs = ComponentPorts(self, InputPort, ArrayInputPort)
        self.outputs = ComponentPorts(self, OutputPort, ArrayOutputPort)
        self.state = ComponentState.NOT_STARTED
        self.owned_packet_count = 0
        self.initialize()

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

    def _run(self):
        # TODO: Handle timeouts

        while not self.is_terminated:

            # TODO: Handle exceptions and set ERROR state
            self.run()

            # Ensure this component is still in running condition
            if all([component.is_terminated for component in self.upstream]):
                # No more packets will ever arrive!
                self.terminate()
            else:
                # More data may arrive
                self.yield_control()

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
    def is_terminated(self):
        '''
        Has this component been terminated?
        '''
        return self.state in (ComponentState.TERMINATED,
                              ComponentState.ERROR)

    def terminate(self):
        if self.is_terminated:
            raise ValueError('Component "%s" is already terminated' % self.name)

        log.debug('Component "%s" is terminating...' % self.name)
        self.state = ComponentState.TERMINATED

        self.runtime.yield_and_terminate()

    def suspend(self):
        self.state = ComponentState.DORMANT
        self.runtime.yield_control()

    def yield_control(self):
        '''
        Yield execution to scheduler.
        '''
        log.debug('Component "%s" is yielding...' % self.name)
        self.runtime.yield_control()

    def __str__(self):
        return ('Component(%s, inputs=%s, outputs=%s)' %
                (self.name, self.inputs, self.outputs))


class InitialPacketCreator(Component):
    '''
    An initial packet (IIP) generator that is connected to an input port
    of a component.

    This should have no input ports, a single output port, and is used to
    make the logic easier.
    '''
    def __init__(self, value):
        self.value = value
        super(InitialPacketCreator, self).__init__('IIP_GEN')

    def initialize(self):
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        self.outputs['OUT'].send(Packet(self.value))


class Graph(Component):
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
        def input_port_valid(input_port):
            return (input_port.optional and
                    input_port.source_port is None)

        self_starters = set()
        for node in self.components:
            # Self-starter nodes should have either no inputs or only have disconnected optional inputs
            if len(node.inputs) == 0:
                self_starters.add(node)
            elif all(map(input_port_valid, node.inputs)):
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

        graph = nx.Graph()

        def _build_nodes(components):
            visited_nodes = set()

            for component in components:
                # Add node
                if component not in visited_nodes:
                    visited_nodes.add(component)
                    graph.add_node(component.name)

                graph.node[component.name]['label'] = component.name

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
                            'label': '%s -> %s' % (output.name, output.target_port.name)
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
