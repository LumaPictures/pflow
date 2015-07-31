from abc import ABCMeta, abstractmethod
import logging
import collections

try:
    from queue import Queue  # 3.x
except ImportError:
    from Queue import Queue  # 2.x

from . import exc

log = logging.getLogger(__name__)


class Packet(object):
    """
    Information packet (IP)
    """
    def __init__(self, value):
        """
        :param value:
        """
        self.value = value
        self._owner = None  # Component that owns this
        self.attrs = {}  # Named attributes

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, value):
        if self._owner is not None:
            raise ValueError("You can not change a packet's owner. "
                             "Create a copy and drop this packet instead.")

        self._owner = value

    def __str__(self):
        return 'Packet(%s)' % self.value


class BracketPacket(Packet):
    """
    Special packet used for bracketing.
    """
    __metaclass__ = ABCMeta

    def __init__(self, key=None):
        """
        :param key: an optional key used for random access bracketing (e.g. building dicts or arrays
                    without the need for strict stack behavior). Represents the begin/end of data
                    for a given key.
        """
        super(BracketPacket, self).__init__(None)
        self.key = key


class StartBracket(BracketPacket):
    """
    Start of bracketed data.
    """
    pass


class EndBracket(BracketPacket):
    """
    End of bracketed data.
    """
    pass


class BasePort(object):
    __metaclass__ = ABCMeta


class Port(BasePort):
    __metaclass__ = ABCMeta

    def __init__(self, name, description=None, optional=False, allowed_types=None):
        """
        :param name: the unique (per-component) name of this port.
        :param description: an optional description of what this port is used for.
        :param optional: is this port optional (i.e. can it be connected to nothing)?
        :param allowed_types: list/set of allowed that can be passed through this port (or empty/None to allow any).
        """
        if not isinstance(name, basestring):
            raise ValueError('name must be a string')

        self.name = name

        if description is not None and not isinstance(description, basestring):
            raise ValueError('description must be a string')

        self.description = description

        if not isinstance(optional, bool):
            raise ValueError('optional must be a bool')

        self.optional = optional  # Is this port optional?

        if allowed_types is not None:
            if not (isinstance(allowed_types, collections.Sequence) and
                    not isinstance(allowed_types, basestring)):
                raise ValueError('allowed_types must be a non-string sequence')

            for type_ in allowed_types:
                if not isinstance(type_, type):
                    raise ValueError('allowed_types: %s is not a type')

            self.allowed_types = set(allowed_types)  # Data types
        else:
            self.allowed_types = set()

        self.component = None  # Owning component
        self._is_open = True

    def supports_type(self, type_):
        if not isinstance(type_, type):
            raise ValueError('type_ must be a type')

        return type_ in self.allowed_types

    def open(self):
        if not self.is_connected():
            raise exc.PortError('%s is disconnected, and can not be opened' % self)

        if self._is_open:
            raise exc.PortError('%s is already open' % self)

        self._is_open = True
        # TODO

    def close(self):
        if not self._is_open:
            raise exc.PortError('%s is already closed' % self)

        self._is_open = False

    def is_open(self):
        return self._is_open

    @abstractmethod
    def is_connected(self):
        pass

    @property
    def id(self):
        if self.component is not None:
            component_name = self.component.name
        else:
            component_name = '(no_component)'

        return '%s.%s' % (component_name, self.name)

    def _check_ready_state(self):
        if not self.is_connected() and not self.optional:
            raise exc.PortError('%s must be connected' % self)

        if not self.is_open():
            raise exc.PortClosedError('%s is closed' % self)

    def __getitem__(self, index):
        raise ValueError('%s is not an array port' % self)

    def __iter__(self):
        raise ValueError('%s is not an array port' % self)

    def __str__(self):
        return '%s(%s%s)' % (self.__class__.__name__,
                             self.id,
                             '*' if self.optional else '')


class InputPort(Port):
    """
    Port that is defined on the input side of a component.
    Leads from either an OutputPort of an upstream component or an initial Packet.
    """
    def __init__(self, name='IN', **kwargs):
        super(InputPort, self).__init__(name, **kwargs)
        self.source_port = None

    def is_connected(self):
        return (self.component is not None and
                self.source_port is not None)

    def receive_packet(self):
        """
        Receive the next Packet from this input port.

        :return: Packet that was received or None if EOF
        """
        from .core import ComponentState

        # Optional port with no connection
        if self.optional and not self.is_connected():
            return None
        else:
            self._check_ready_state()

        runtime = self.component.runtime
        packet = runtime.receive_port(self.component, self.name)

        return packet

    def receive(self):
        packet = self.receive_packet()
        if packet:
            value = packet.value
            self.component.drop(packet)
            return value

    def close(self):
        if self.is_open():
            self.component.runtime.close_input_port(self.component, self.name)

        super(InputPort, self).close()


class ArrayPort(BasePort):
    __metaclass__ = ABCMeta

    def __init__(self, name, max_ports, **kwargs):
        if not isinstance(name, basestring):
            raise ValueError('name must be a string')

        self._name = name
        self._max_ports = max_ports
        self._kwargs = kwargs

        self._ports = []
        self._allocate()

    def _allocate(self):
        """
        Allocate array port.
        """
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
    """
    Port that is defined on the output side of a Component.
    Leads into an InputPort of a downstream component.
    """
    def __init__(self, name='OUT', **kwargs):
        super(OutputPort, self).__init__(name, **kwargs)
        self._bracket_depth = 0
        self.target_port = None

    def is_connected(self):
        return (self.component is not None and
                self.target_port is not None)

    def send_packet(self, packet):
        """
        Send a single Packet over this output port.

        :param packet: the Packet to send over this output port.
        """
        from .core import ComponentState

        if self.optional and not self.is_connected():
            return
        else:
            self._check_ready_state()

        runtime = self.component.runtime
        runtime.send_port(self.component, self.name, packet)

    def send(self, value):
        packet = self.component.create_packet(value)
        self.send_packet(packet)

    def start_bracket(self):
        self._bracket_depth += 1

        packet = StartBracket()
        packet.owner = self.component
        self.send_packet(packet)

    def end_bracket(self):
        self._bracket_depth -= 1
        if self._bracket_depth < 0:
            raise ValueError('end_bracket() called too many times on %s' % self)

        packet = EndBracket()
        packet.owner = self.component
        self.send_packet(packet)

    def close(self):
        if self._bracket_depth != 0:
            raise ValueError('There are %d open brackets on %s' % self)

        if self.is_open():
            self.component.runtime.close_output_port(self.component, self.name)

        super(OutputPort, self).close()


class ArrayOutputPort(ArrayPort):
    @abstractmethod
    def get_port_class(self):
        return InputPort


class PortRegistry(object):
    """
    Per-component port registry descriptor.
    """
    def __init__(self, component, *required_superclasses):
        self._ports = {}
        self._component = component

        if not all(map(lambda c: issubclass(c, BasePort), required_superclasses)):
            raise ValueError('required_superclass must be Port subclass')

        self._required_superclasses = required_superclasses

    def add(self, *ports):
        """
        Add a new port to the registry.
        """
        for port in ports:
            if not isinstance(port, self._required_superclasses):
                raise ValueError('%s must be an instance of: %s' %
                                 (port, ', '.join([c.__name__ for c in self._required_superclasses])))

            if port.name in self._ports:
                raise ValueError('%s already exists' % port)

            if port.component is not None and port.component != self._component:
                raise ValueError('%s is already attached to %s' %
                                 (port, port.component))

            port.component = self._component
            # FIXME: this needs to be ordered
            self._ports[port.name] = port

        return self

    def __getitem__(self, port_name):
        """
        Get a port from the registry by name (using [] notation).
        """
        if not isinstance(port_name, basestring):
            raise ValueError('key must be a string')

        if port_name not in self._ports:
            raise AttributeError('%s does not have a port named "%s"' %
                                 (self._component, port_name))

        return self._ports[port_name]

    def __setitem__(self, port_name, port):
        if port.name is None:
            port.name = port_name
        elif not isinstance(port.name, basestring):
            raise ValueError('key must be a string')

        self.add(port)

    def __iter__(self):
        return iter(self._ports.values())

    def __len__(self):
        return len(self._ports)

    def __str__(self):
        return '(%s)' % ', '.join(map(str, self._ports.values()))
