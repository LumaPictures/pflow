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
        self._is_open = True

    def open(self):
        if not self.is_connected:
            raise exc.PortError('Port "%s" is disconnected (%s, %s, %s), and can not be opened' %
                                (self._port_name, self.component, self.target_port, self.source_port))

        if self._is_open:
            raise exc.PortError('Port "%s" is already open' % self._port_name)

        self._is_open = True
        # TODO

    def close(self):
        if not self._is_open:
            raise exc.PortError('Port "%s" is already closed' % self._port_name)

        self._is_open = False

    @property
    def is_open(self):
        return self._is_open

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

        # if self.auto_open:
        #     log.debug('Opening Port "%s" (auto)' % self._port_name)
        #     self.open()

    @property
    def is_connected(self):
        if self.component is None:
            return False
        elif isinstance(self, InputPort):
            return self.source_port is not None
        elif isinstance(self, OutputPort):
            return self.target_port is not None

    @property
    def _port_name(self):
        if self.component is not None:
            component_name = self.component.name
        else:
            component_name = '(no_component)'

        return '%s.%s' % (component_name, self.name)

    def _check_ready_state(self):
        if not self.is_connected and not self.optional:
            raise exc.PortError('Port "%s" must be connected' % self._port_name)
        if not self.is_open:
            raise exc.PortClosedError('Port "%s" is closed' % self._port_name)

    def __getitem__(self, index):
        raise ValueError('Port "%s" is not an array port' % self._port_name)

    def __iter__(self):
        raise ValueError('Port "%s" is not an array port' % self._port_name)

    def __str__(self):
        return '%s(%s%s)' % (self.__class__.__name__,
                             self._port_name,
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

        runtime = self.component.runtime
        packet = runtime.receive(self)

        # TODO: claim ownership
        # TODO: increment refcount

        return packet

    def receive_value(self):
        packet = self.receive()
        if packet:
            return packet.value


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
        if self.optional and not self.is_connected:
            return
        else:
            self._check_ready_state()

        runtime = self.component.runtime
        runtime.send(packet, self.target_port)

        # TODO: decrement refcount

    def send_value(self, value):
        packet = self.component.create_packet(value)
        self.send(packet)


class ArrayOutputPort(ArrayPort):
    @abstractmethod
    def get_port_class(self):
        return InputPort


class PortRegistry(object):
    '''
    Per-component port registry descriptor.
    '''
    def __init__(self, component, *required_superclasses):
        self._ports = {}
        self._component = component

        if not all(map(lambda c: issubclass(c, BasePort), required_superclasses)):
            raise ValueError('required_superclass must be Port subclass')

        self._required_superclasses = required_superclasses

    def add(self, *ports):
        '''
        Add a new port to the registry.
        '''
        for port in ports:
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

    def __getitem__(self, port_name):
        '''
        Get a port from the registry by name (using [] notation).
        '''
        if port_name not in self._ports:
            raise AttributeError('Component "%s" does not have a port named "%s"' %
                                 (self._component.name, port_name))

        return self._ports[port_name]

    def __setitem__(self, port_name, port):
        if port.name is None:
            port.name = port_name

        self.add(port)

    def __iter__(self):
        return iter(self._ports.values())

    def __len__(self):
        return len(self._ports)

    def __str__(self):
        return '(%s)' % ', '.join(map(str, self._ports.values()))
