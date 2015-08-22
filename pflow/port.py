from abc import ABCMeta, abstractmethod
import logging
import collections
import copy

try:
    from queue import Queue  # 3.x
except ImportError:
    from Queue import Queue  # 2.x

from .packet import (EndOfStream, Packet, StartSubStream, EndSubStream,
                     StartMap, EndMap, SwitchMapNamespace)
from . import exc

log = logging.getLogger(__name__)


class BasePort(object):
    __metaclass__ = ABCMeta


class Port(BasePort):
    __metaclass__ = ABCMeta

    def __init__(self, name, description=None, optional=True,
                 allowed_types=None, default=None):
        """
        Parameters
        ----------
        name : str
            the unique (per-component) name of this port.
        description : str
            an optional description of what this port is used for.
        optional : bool
            whether this port is optional (i.e. can it be connected to nothing)
        allowed_types : list of type
            allowed types that can be passed through this port (or empty/None
            to allow any).
        """
        if not isinstance(name, basestring):
            raise ValueError('name %s must be a string')

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

        self.default = default

        self.component = None  # Owning component
        self._is_open = True
        self.proxied_port = None

        self.log = logging.getLogger('%s.%s' % (self.__class__.__module__,
                                                self.__class__.__name__))

    def supports_type(self, type_):
        if not isinstance(type_, type):
            raise ValueError('type_ must be a type')

        return type_ in self.allowed_types

    def open(self):
        if not self.is_connected():
            raise exc.PortError(self, 'port is disconnected, and can not be '
                                      'opened')

        if self._is_open:
            raise exc.PortError(self, 'port is already open')

        self._is_open = True

    def close(self):
        if not self._is_open:
            raise exc.PortError(self, 'port is already closed')

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
            raise exc.PortError(self, 'port must be connected')

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

    Leads from either an OutputPort of an upstream component or an initial
    Packet.
    """
    def __init__(self, name='IN', max_queue_size=None, **kwargs):
        super(InputPort, self).__init__(name, **kwargs)

        self.source_port = None

        # If max_queue_size is set, it limits the queue size so that a
        # component can apply backpressure by causing the upstream component
        # to block on send_packet() when the queue is full.
        self.max_queue_size = max_queue_size

    def is_connected(self):
        return (self.component is not None and
                (self.source_port is not None or
                 self.proxied_port is not None))

    def receive_packet(self, timeout=None):
        """
        Receive the next Packet from this input port.

        Returns
        -------
        packet : ``Packet`` or ``EndOfStream``
            Packet that was received, or ``EndOfStream``
        """
        # Optional port with no connection
        if self.optional and not self.is_connected():
            return EndOfStream
        else:
            self._check_ready_state()

        packet = self.component.executor.receive_port(self.component,
                                                      self.name,
                                                      timeout=timeout)

        return packet

    def receive(self, timeout=None):
        """
        Receive the value of the next Packet from this input port.

        This unpacks the `value` attribute of the packet and drops the packet.

        Returns
        -------
        value : object or ``EndOfStream``
            value of the packet that was received, or ``EndOfStream``
        """
        packet = self.receive_packet(timeout=timeout)
        if packet is EndOfStream:
            self.log.debug('{} is closed (component={}, source_port={}, proxied_port={})'.format(
                           self, self.component, self.source_port, self.proxied_port))
            return packet
        else:
            value = packet.value
            self.component.drop_packet(packet)
            return value

    def close(self):
        if self.is_open():
            self.component.executor.close_input_port(self.component, self.name)

        super(InputPort, self).close()


# FIXME: make this a mixin and have ArrayInputPort inherit from InputPort?
# would give cleaner validation: isinstance(InputPort).
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
            self._ports.append(port_class('{}_{:d}'.format(self._name, i),
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
        Send a single packet over this output port.

        Parameters
        ----------
        packet : ``Packet``
            the Packet to send over this output port.
        """
        if self.optional and not self.is_connected():
            return EndOfStream
        else:
            self._check_ready_state()

        if not self.is_open():
            raise exc.PortClosedError(self)

        if packet is EndOfStream:
            raise ValueError('You can not send an EndOfStream downstream!')

        if not isinstance(packet, Packet):
            raise ValueError('packet must be a Packet instance')

        # All packets must have an owner. Automatically set owner to this port's
        # component if none set already.
        if packet.owner is None:
            self.log.warn('No owner set on packet {}. Setting it to {}.'.format(packet, self.component))
            packet.owner = self.component
            self.component.owned_packet_count += 1

        executor = self.component.executor
        executor.send_port(self.component, self.name, packet)

    def send(self, value):
        packet = self.component.create_packet(value)
        self.send_packet(packet)

    def start_substream(self):
        self._bracket_depth += 1

        packet = StartSubStream()
        packet.owner = self.component
        packet.owner.owned_packet_count += 1

        self.send_packet(packet)

    def end_substream(self):
        self._bracket_depth -= 1
        if self._bracket_depth < 0:
            raise ValueError('end_substream / end_map called too many times '
                             'on {}'.format(self))

        packet = EndSubStream()
        packet.owner = self.component
        packet.owner.owned_packet_count += 1

        self.send_packet(packet)

    def start_map(self):
        self._bracket_depth += 1

        packet = StartMap()
        packet.owner = self.component
        self.send_packet(packet)

    def end_map(self):
        self._bracket_depth -= 1
        if self._bracket_depth < 0:
            raise ValueError('end_substream / end_map called too many times '
                             'on {}'.format(self))

        packet = EndMap()
        packet.owner = self.component
        self.send_packet(packet)

    def switch_map_namespace(self, key):
        packet = SwitchMapNamespace(key)
        packet.owner = self.component
        self.send_packet(packet)

    def close(self):
        if self._bracket_depth != 0:
            raise ValueError('There are {:d} open brackets on '
                             '{}'.format(self._bracket_depth, self))

        if self.is_open():
            self.component.executor.close_output_port(self.component,
                                                      self.name)

        super(OutputPort, self).close()


class ArrayOutputPort(ArrayPort):
    @abstractmethod
    def get_port_class(self):
        return InputPort


class PortRegistry(object):
    """
    Per-component port registry descriptor.
    """
    def __init__(self, component, port_type, array_port_type):
        self._ports = collections.OrderedDict()
        self._component = component
        self._port_type = port_type
        self._array_port_type = array_port_type
        self._required_superclasses = (port_type, array_port_type)

        if not issubclass(port_type, BasePort):
            raise ValueError('port_type must be Port subclass')
        if not issubclass(array_port_type, BasePort):
            raise ValueError('array_port_type must be Port subclass')

    # FIXME: move this to Component and auto-delegate to self.inputs vs self.outputs based on port type
    def add_ports(self, *ports):
        """
        Add a new port to the registry.
        """
        for port in ports:
            if not isinstance(port, self._required_superclasses):
                raise ValueError('{} must be an instance of: {}'.format(
                                 port, ', '.join([c.__name__ for c in self._required_superclasses])))

            if port.name in self._ports:
                raise ValueError('{} already exists'.format(port))

            if port.component is not None and port.component != self._component:
                raise ValueError('{} is already attached to {}'.format(
                                 port, port.component))

            port.component = self._component
            self._ports[port.name] = port

        return self

    def add(self, name, **kwargs):
        """
        Add a new port to the registry.
        """
        if not isinstance(name, basestring):
            raise ValueError('port name must be a string')

        port = self._port_type(name, **kwargs)
        self.add_ports(port)
        return port

    def export(self, name, exported_port):
        if not isinstance(exported_port, self._port_type):
            raise ValueError('Unable to export port {} because exported_port '
                             'must be a {}'.format(name,
                                                   self._port_type))

        if exported_port.proxied_port is self:
            raise ValueError('Unable to export port {} because exported_port {} '
                             'is already proxied to self'.format(name))

        if exported_port.is_connected():
            raise ValueError('exported_port {} is already connected!'.format(exported_port))

        port = copy.copy(exported_port)
        port.name = name
        port.component = self._component

        if isinstance(port, InputPort):
            port.source_port = None
            exported_port.source_port = port
            port.proxied_port = exported_port
        elif isinstance(port, OutputPort):
            port.target_port = None
            exported_port.target_port = port
            raise NotImplementedError('OutputPorts cant be exported yet')

        self.add_ports(port)
        return port

    def __getitem__(self, port_name):
        """
        Get a port from the registry by name (using [] notation).
        """
        from .states import ComponentState

        if not isinstance(port_name, basestring):
            raise ValueError('key must be a string')

        try:
            return self._ports[port_name]
        except KeyError:
            if self._component.state == ComponentState.NOT_INITIALIZED:
                raise exc.ComponentStateError(self._component,
                                              'Attempted to access port "{}" before component was '
                                              'initialized'.format(port_name))
            else:
                raise AttributeError('{} does not have a port named "{}"'.format(
                                     self._component, port_name))

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
