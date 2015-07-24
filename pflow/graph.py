from abc import ABCMeta, abstractmethod
import logging
try:
    from queue import Queue  # 3.x
except ImportError:
    from Queue import Queue  # 2.x
from enum import Enum  # 3.x (or enum34 backport

from . import exc

log = logging.getLogger(__name__)


class BasePort(object):
    pass


class Port(BasePort):
    __metaclass__ = ABCMeta

    def __init__(self, name, optional=False):
        self.name = name
        self.sender = None  # Sending Component
        self.receiver = None  # Receiving Component
        self.type_ = None  # Data type
        self.optional = False  # Is this port optional?
        self._buffer = Queue()  # TODO: This can be an mp.Queue() or a message queue depending on runtime
        self._is_open = False

    def open(self):
        if not self.is_connected:
            raise ValueError('You can not open a disconnected Port')

        if self._is_open:
            raise ValueError('Port is already open')

        self._is_open = True
        # TODO

    def close(self):
        if self._is_open:
            raise ValueError('Port is already closed')

        self._is_open = False

    @property
    def is_open(self):
        return self._is_open

    def is_connected(self):
        return (self.sender is not None and
                self.receiver is not None)


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
        # TODO: claim ownership
        # TODO: increment refcount
        log.debug('Receiving packet over port %s' % self.name)


class ArrayPort(BasePort):
    __metaclass__ = ABCMeta

    def __init__(self, name, max_ports, **kwargs):
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
        # TODO: decrement refcount
        log.debug('Sending packet over port %s' % self.name)


class ArrayOutputPort(ArrayPort):
    @abstractmethod
    def get_port_class(self):
        return InputPort


class Packet(object):
    def __init__(self, value):
        self._value = value
        self._owner = None  # Component that owns this
        self.type_ = None  # TODO
        self.attrs = {}  # Named attributes

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        # TODO: unset old owner by decrementing self._owner.owned_packet_count
        self._owner = value

    @property
    def value(self):
        # TODO: typecast to self.type_
        return self._value


class BracketPacket(Packet):
    '''
    Special packet used for bracketing.
    '''
    pass


class StartBracket(BracketPacket):
    pass


class EndBracket(BracketPacket):
    pass


class AddressablePorts(object):
    '''
    Descriptor that allows addressing ports by name (as attributes).
    '''
    def __init__(self, *required_superclasses):
        self._ports = {}

        if not all(map(lambda c: issubclass(c, BasePort), required_superclasses)):
            raise ValueError('required_superclass must be Port subclass')

        self._required_superclasses = required_superclasses

    def add(self, port):
        if not isinstance(port, self._required_superclasses):
            raise ValueError('port "%s" must be an instance of: %s' %
                             (port.name, ', '.join([c.__name__ for c in self._required_superclasses])))

        if port.name in self._ports:
            raise ValueError('port "%s" already exists' % port.name)

        self._ports[port.name] = port
        return self

    def __get__(self, port_name):
        if port_name not in self._ports:
            raise AttributeError('Component does not have a port named "%s"' %
                                 port_name)

        return self._ports[port_name]

    def __iter__(self):
        return iter(self._ports.values())


class ComponentState(Enum):
    NOT_STARTED = 1
    ACTIVE = 2
    DORMANT = 3
    TERMINATED = 4
    ERROR = 5


class Component(object):
    __metaclass__ = ABCMeta

    def __init__(self, name):
        self.name = name
        self.inputs = AddressablePorts(InputPort, ArrayInputPort)
        self.outputs = AddressablePorts(OutputPort, ArrayOutputPort)
        self.state = ComponentState.NOT_STARTED
        self.owned_packet_count = 0
        self.define()

    @abstractmethod
    def define(self):
        '''
        Initialization code necessary to define this component.
        '''
        pass

    def _run(self):
        while self.state not in (ComponentState.TERMINATED,
                                 ComponentState.ERROR):
            self.run()
            self.yield_control()
            # TODO: Check if component is still runnable (has non-terminated parents)

    @abstractmethod
    def run(self):
        pass

    def drop(self, packet):
        '''
        Drop a Packet.
        '''
        pass

    def validate(self):
        '''
        Validate component state and the state of all its ports.
        Raises a ComponentInvalidError if there was a problem.
        '''
        # raise exc.ComponentInvalidError, 'This component is invalid!'
        log.debug('Validating component %s...' % self.__class__.__name__)

        # TODO: Ensure there's at least 1 port defined
        # TODO: Ensure all ports are connected
        # TODO: Ensure all non-optional ports have connections

        return self

    def terminate(self):
        log.debug('Terminating component %s...' % self.__class__.__name__)
        self.state = ComponentState.TERMINATED

    def yield_control(self):
        '''
        Yield execution to scheduler.
        '''
        log.debug('Component %s is going dormant...' % self.__class__.__name__)
        self.state = ComponentState.DORMANT
        # TODO: Yield to scheduler or suspend thread


class InitialPacketCreator(Component):
    '''
    An initial packet (IIP) generator that is connected to an input port
    of a component.

    This should have no input ports, a single output port, and is used to
    make the logic easier.
    '''
    def __init__(self, value):
        self.value = value
        super(InitialPacketCreator, self).__init__(inputs=None,
                                                   outputs=OutputPort())


class Graph(Component):
    def __init__(self, *args, **kwargs):
        self.components = set()
        self.ports = set()
        super(Graph, self).__init__(*args, **kwargs)

    def add_component(self, component):
        self.components.add(component)

        for port in component.inputs:
            self.ports.add(port)

        for port in component.outputs:
            self.ports.add(port)

        return self

    def run(self):
        self.validate()
        log.debug('Executing graph...')

        # TODO: initiate all self-starters
        pass

    def validate(self):
        log.debug('Validating graph...')

        # TODO: validate all components
        # TODO: ensure all components are connected
        # TODO: ensure all edges have components

        return self

    def load_fbp_string(self, fbp_script):
        # TODO: parse fbp string and build graph

        return self

    def load_fbp_file(self, file_path):
        with open(file_path, 'r') as f:
            self.load_fbp_string(f.read())

        return self


class SubGraph(Graph):
    '''
    SubGraphs are just like Graphs but have slightly different run() semantics.
    '''
    def run(self):
        pass

