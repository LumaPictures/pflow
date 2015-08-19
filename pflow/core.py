from abc import ABCMeta, abstractmethod
import logging
import json
import inspect

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

from . import utils
from . import exc
from . import parsefbp
from .port import (PortRegistry, InputPort, OutputPort, ArrayInputPort,
                   ArrayOutputPort)
from .packet import (Packet, EndOfStream, ControlPacket, StartSubStream,
                     EndSubStream, StartMap, EndMap, SwitchMapNamespace)
from .states import (ComponentState, assert_component_state,
                     assert_not_component_state)

log = logging.getLogger(__name__)


def keepalive(fn):
    """
    Decorator that tells the runtime to invoke the component's run()
    for each incoming packet until the graph is exhausted or the component
    is explicitly terminated.
    """
    if fn.func_name != 'run':
        raise ValueError('The keepalive decorator was only intended for the '
                         'Component.run() method')

    # FIXME: remove keepalive knowledge from the executor and use something like this:
    # @functools.wraps(fn)
    # def wrapper(self):
    #     while not self.is_terminated:
    #         fn(self)
    # return wrapper
    # See more notes in executors.base.GraphExecutor._create_component_runner

    fn._component_keepalive = True
    return fn


class Component(object):
    """
    Component instances are "process" nodes in a flow-based digraph.

    Each Component has zero or more input and output ports, which are
    connected to other Components through Ports.
    """
    __metaclass__ = ABCMeta

    # Valid state transitions for components.
    #
    # See ../docs/states.graphml for how this is visually represented.
    # Please keep this list of edges in sync with the graphml and README.md
    # docs!
    _valid_transitions = frozenset([
        (ComponentState.NOT_INITIALIZED, ComponentState.INITIALIZED),

        (ComponentState.INITIALIZED, ComponentState.ACTIVE),
        # This may happen when the graph shuts down and a component hasn't been
        # run yet:
        (ComponentState.INITIALIZED, ComponentState.TERMINATED),

        (ComponentState.ACTIVE, ComponentState.SUSP_SEND),
        (ComponentState.ACTIVE, ComponentState.SUSP_RECV),
        (ComponentState.ACTIVE, ComponentState.TERMINATED),
        (ComponentState.ACTIVE, ComponentState.ERROR),

        (ComponentState.SUSP_SEND, ComponentState.ACTIVE),
        (ComponentState.SUSP_SEND, ComponentState.ERROR),

        (ComponentState.SUSP_RECV, ComponentState.ACTIVE),
        (ComponentState.SUSP_RECV, ComponentState.ERROR),
        (ComponentState.SUSP_RECV, ComponentState.TERMINATED)
    ])

    _state = ComponentState.NOT_INITIALIZED

    def __init__(self, name, initialize=True):
        """
        Parameters
        ---------
        name : str
            unique name of this component instance within the graph.
        """
        if not isinstance(name, basestring):
            raise ValueError('name must be a string')

        self.name = name
        self.inputs = PortRegistry(self, InputPort, ArrayInputPort)
        self.outputs = PortRegistry(self, OutputPort, ArrayOutputPort)
        self.log = logging.getLogger('%s.%s(%s)' % (self.__class__.__module__,
                                                    self.__class__.__name__,
                                                    self.name))
        if initialize:
            self.initialize()
            self.state = ComponentState.INITIALIZED

        self.executor = None
        # FIXME: not actually used:
        self.stack = queue.LifoQueue()  # Used for simple bracket packets
        self.owned_packet_count = 0

    @property
    def state(self):
        return self._state

    @state.setter
    def state(self, new_state):
        if not isinstance(new_state, ComponentState):
            raise ValueError('new_state must be a value from the '
                             'ComponentState enum')

        old_state = self._state

        if old_state == new_state:
            return

        # Ensure state transition is a valid one.
        if (old_state, new_state) not in self._valid_transitions:
            raise exc.ComponentStateError(
                'Invalid state transition for {}: {} -> {}'.format(
                    self, old_state.value, new_state.value))

        self._state = new_state

        self.log.debug('State transitioned from {} -> {}'.format(
            old_state.value, new_state.value))

        # If supported by interpreter, show the caller to this property method
        # to determine where the state was changed from.
        curr_frame = inspect.currentframe()
        if curr_frame is not None:
            # Python interpreter has stack support.
            frame_limit = 3
            frames = inspect.getouterframes(curr_frame, 2)
            caller_frames = frames[1:frame_limit + 1]
            if len(caller_frames) > 1:
                # (frame, filename, lineno, function, code_context, index)
                indent = '\t' * 4
                caller_stack = ('\n' + indent).join(
                    '{}() in {}:{:d}'.format(fr[3], fr[1], fr[2])
                    for fr in caller_frames)
                self.log.debug('State was changed by (last {:d} frames):\n'
                               '{}{}'.format(frame_limit, indent,
                                             caller_stack))

        # TODO: Fire a transition event

    # @abstractmethod
    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def initialize(self):
        """
        Initialization code necessary to define this component.
        """
        pass

    @abstractmethod
    @assert_component_state(ComponentState.INITIALIZED)
    def run(self):
        """
        This is where all work is performed during component execution.
        """
        pass

    # @assert_component_state(ComponentState.TERMINATED,
    #                         ComponentState.ERROR)
    def destroy(self):
        """
        Implementations can override this to call any cleanup code when the
        component has transitioned to a TERMINATED state and is about to be
        destroyed.
        """
        self.log.debug('Destroyed')

    @assert_not_component_state(ComponentState.TERMINATED,
                                ComponentState.ERROR)
    def create_packet(self, value):
        """
        Create a new packet.

        This creates a `Packet` instance and sets this component as the owner.
        Once created, a packet which is no longer needed must be explicitly
        destroyed using `drop_packet`, otherwise it is considered a leak and
        will result in warnings when the graph terminates.

        Parameters
        ----------
        value : object
            value for the packet.

        Returns
        -------
        packet : ``Packet``
            newly created packet
        """
        if isinstance(value, (Packet, Component)):
            raise ValueError("Attempting to create a packet out of a {}: "
                             "{!r}".format(value.__class__.__name__, value))

        packet = Packet(value)
        packet.owner = self

        self.owned_packet_count += 1
        return packet

    # FIXME: shouldn't this be:
    # @assert_component_state(ComponentState.ACTIVE)
    @assert_not_component_state(ComponentState.TERMINATED,
                                ComponentState.ERROR)
    def drop_packet(self, packet):
        """
        Drop a Packet.

        Parameters
        ----------
        packet : ``Packet``
            the Packet to drop.
        """
        if not isinstance(packet, Packet):
            raise ValueError('packet must be a Packet')

        owner = packet.owner

        if owner is None:
            self.log.warn('Dropping packet {} with no owner set!'.format(packet))
        elif owner.owned_packet_count > 0:
            owner.owned_packet_count -= 1
        else:
            raise ValueError('Reference count error: {} was unable to drop {} because its owner {} '
                             'already has an owned_packet_count of 0'.format(self,
                                                                             packet,
                                                                             owner))

        del packet

    # FIXME: This would be clearer as a method (properties should be reserved for attribute-like values)
    @property
    def is_terminated(self):
        """
        Returns whether the component has been terminated.

        Returns
        -------
        is_terminated : bool
            whether the component has been terminated.
        """
        return self.state in (ComponentState.TERMINATED,
                              ComponentState.ERROR)

    def is_alive(self):
        """
        Returns whether the component is still alive (e.g. has not yet been
        terminated).

        Returns
        -------
        is_alive : bool
            whether the component is still alive.
        """
        return not self.is_terminated

    # FIXME: This would be clearer as a method (properties should be reserved for attribute-like values)
    @property
    def is_suspended(self):
        """
        Returns whether the component is in a suspended state.

        Returns
        -------
        is_suspended : bool
            whether the component is suspended.
        """
        return self.state in (ComponentState.SUSP_RECV,
                              ComponentState.SUSP_SEND)

    @assert_not_component_state(ComponentState.TERMINATED,
                                ComponentState.ERROR)
    def terminate(self, ex=None):
        """
        Terminate execution for this component.
        This will not terminate upstream components!
        """
        if self.is_terminated:
            return

        if ex is not None:
            if not isinstance(ex, Exception):
                raise ValueError('If an exception is passed, it must be an'
                                 'instance of Exception')

            self.log.error('Abnormally terminating because of {}...'.format(
                           ex.__class__.__name__))
            self.state = ComponentState.ERROR
        else:
            self.state = ComponentState.TERMINATED

        self.executor.terminate_thread(self)

    @assert_component_state(ComponentState.ACTIVE)
    def suspend(self, seconds=None):
        """
        Yield execution to scheduler.
        This may be used in place of `time.sleep()`

        Parameters
        ----------
        seconds : float
            minimum number of seconds to sleep. (optional)
        """
        self.executor.suspend_thread(seconds)

    def __str__(self):
        return '{}({!r})'.format(self.__class__.__name__, self.name)


class InitialPacketGenerator(Component):
    """
    An initial packet (IIP) generator that is connected to an input port
    of a component.

    This should have no input ports, a single output port, and generates a
    single packet before terminating.
    """
    def __init__(self, value):
        super(InitialPacketGenerator, self).__init__(
            'IIP_GEN_{}'.format(utils.random_id()))
        self.value = value

    def initialize(self):
        self.outputs.add('OUT')

    def run(self):
        self.outputs['OUT'].send(self.value)


class Graph(Component):
    """
    Execution graph.

    This can also be used as a subgraph for composite components.
    """
    __metaclass__ = ABCMeta

    def __init__(self, *args, **kwargs):
        self.components = set()  # Nodes
        super(Graph, self).__init__(*args, **kwargs)

    @classmethod
    def get_upstream(cls, component):
        """
        Immediate upstream components.

        Parameters
        ----------
        component : ``Component``
            the Component to check.

        Returns
        -------
        components : the upstream components
            set of ``Component``
        """
        upstream = set()

        for port in component.inputs:
            if port.is_connected():
                upstream.add(port.source_port.component)

        return upstream

    @classmethod
    def get_downstream(cls, component):
        """
        Immediate downstream components.

        Parameters
        ----------
        component : ``Component``
            the Component to check.

        Returns
        -------
        components : the downstream components
            set of ``Component``
        """
        downstream = set()

        for port in component.outputs:
            if port.is_connected():
                downstream.add(port.target_port.component)

        return downstream

    @classmethod
    def is_upstream_terminated(cls, component):
        """
        Are all of a component's upstream components terminated?

        Parameters
        ----------
        component : ``Component``
            the Component to check.

        Returns
        -------
        is_terminated : bool
            whether or not the upstream components have been terminated.
        """
        return all(c.is_terminated for c in cls.get_upstream(component))

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def add_component(self, component):
        """
        Add a component to the graph.

        Parameters
        ----------
        component : ``Component``
            the Component to add.

        Returns
        -------
        component : ``Component``
            the added Component.
        """
        if not isinstance(component, Component):
            raise ValueError('component must be a Component instance')

        # Already added?
        if component in self.components:
            return component

        # Unique name?
        used_names = utils.pluck(self.components, 'name')
        if component.name in used_names:
            raise ValueError('Component name "{}" has already been used in '
                             'this graph'.format(component.name))

        self.components.add(component)
        return component

    def get_component(self, name):
        for component in self.components:
            if component.name == name:
                return component
        raise ValueError('Component name "{}" does not exist in this graph'.format(name))

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def remove_component(self, component):
        """
        Remove a component from the graph.
        Also disconnects its ports.

        Parameters
        ----------
        component : ``Component``
            the Component to remove.
        """
        if isinstance(component, basestring):
            component_name = component
            for c in self.components:
                if c.name == component_name:
                    component = c
                    break

        if not isinstance(component, Component):
            raise ValueError('component must either be a Component object or '
                             'the name of a component to remove')

        for outport in component.outputs:
            self.disconnect(outport)

        for inport in component.inputs:
            self.disconnect(inport)

        self.components.remove(component)

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def set_initial_packet(self, port, value):
        """
        Adds an Initial Information Packet (IIP) to the InputPort of a Component.

        Parameters
        ----------
        port : ``port.InputPort``
            the input port to add the IIP to.
        value : object
            the value to set on the IIP (will automatically be wrapped in a
            Packet).

        Returns
        -------
        component : ``core.InitialPacketGenerator``
            the component that gets created and attached to the port.
        """
        if not isinstance(port, InputPort):
            raise ValueError('port must be an InputPort')

        iip = InitialPacketGenerator(value)
        self.connect(iip.outputs['OUT'], port)

        return iip

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def unset_initial_packet(self, port):
        """
        Removes an Initial Information Packet (IIP) from the InputPort of a
        Component.

        Parameters
        ----------
        port : ``port.InputPort``
            the input port to remove the IIP from.
        """
        if not isinstance(port, InputPort):
            raise ValueError('port {} must be an InputPort'.format(port))

        if not port.is_connected():
            raise ValueError('port {} has no IIP because it is '
                             'disconnected'.format(port))

        iip_gen = port.source_port.component
        if not isinstance(iip_gen, InitialPacketGenerator):
            raise ValueError('port {} is connected, but not to '
                             'an IIP'.format(port))

        self.remove_component(iip_gen)

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def set_port_defaults(self, component):
        """
        Create a default initial packets on all of the component's optional
        unconnected ports.

        Parameters
        ----------
        component : ``Component``
            the Component to set port defaults on.
        """
        for port in component.inputs:
            if port.optional and not port.is_connected():
                self.set_initial_packet(port, port.default)

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def connect(self, source_output_port, target_input_port):
        """
        Connect components by their ports.

        Parameters
        ----------
        source_output_port : ``port.OutputPort``
            the output port on the source component.
        target_input_port : ``port.InputPort``
            the input port on the target component.
        """
        if not isinstance(source_output_port, (OutputPort, ArrayOutputPort)):
            raise ValueError('source_output_port must be an output port')

        if not isinstance(target_input_port, (InputPort, ArrayInputPort)):
            raise ValueError('target_input_port must be an input port')

        if target_input_port.source_port is not None:
            raise exc.PortError('target_input_port is already connected to '
                                'another source')

        log.debug('{} connected to {}'.format(source_output_port,
                                              target_input_port))

        self.add_component(source_output_port.component)
        self.add_component(target_input_port.component)

        # FIXME: make these private?
        source_output_port.target_port = target_input_port
        target_input_port.source_port = source_output_port

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def disconnect(self, port):
        """
        Disconnect a Component's Port.
        When you disconnect one end, the other end will be disconnected as
        well.

        Parameters
        ----------
        port : ``port.Port``
            the port to disconnect.
        """
        if not port.is_connected():
            if isinstance(port, OutputPort):
                log.debug('%s disconnected from %s' % (port, port.target_port))
                port.target_port = None
            elif isinstance(port, InputPort):
                log.debug('%s disconnected from %s' % (port, port.source_port))
                port.source_port = None

    @property
    def self_starters(self):
        """
        Returns a set of all self-starter components.
        """
        # Has only disconnected optional inputs
        def is_self_starter_input(input_port):
            return (input_port.optional and
                    input_port.source_port is None)

        self_starters = set()
        for node in self.components:
            # Self-starter nodes should have either no inputs or only
            # have disconnected optional inputs.
            if len(node.inputs) == 0:
                # No inputs
                self_starters.add(node)
            elif all(map(is_self_starter_input, node.inputs)):
                self_starters.add(node)

        return self_starters

    def run(self):
        raise RuntimeError('Instead of calling Graph.run(), please use a '
                           'GraphRuntime to run this Graph')

    @property
    def is_terminated(self):
        """
        Has this graph been terminated?

        Returns
        -------
        is_terminated : bool
            whether the graph has terminated.
        """
        return all([component.is_terminated for component in self.components])

    # TODO: move all serializers to their own module / abstract class

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def load_fbp_string(self, fbp_script):
        if not isinstance(fbp_script, basestring):
            raise ValueError('fbp_script must be a string')

        # TODO: parse fbp string and build graph
        # raise NotImplementedError
        pass

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def load_fbp_file(self, file_path):
        if not isinstance(file_path, basestring):
            raise ValueError('file_path must be a string')

        with open(file_path, 'r') as f:
            self.load_fbp_string(f.read())

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def load_json_dict(self, json_dict):
        # TODO: parse json dict and build graph
        raise NotImplementedError

    @assert_component_state(ComponentState.NOT_INITIALIZED)
    def load_json_file(self, file_path):
        if not isinstance(file_path, basestring):
            raise ValueError('file_path must be a string')

        with open(file_path) as f:
            json_dict = json.loads(f.read())
            self.load_json_dict(json_dict)

    def write_graphml(self, file_path):
        """
        Writes this Graph as a *.graphml file.

        This is useful for debugging network configurations that are hard
        to visualize purely with code.

        :param file_path: the file to write to (should have a .graphml extension)
        """
        import networkx as nx

        if not isinstance(file_path, basestring):
            raise ValueError('file_path must be a string')

        graph = nx.DiGraph()

        def build_nodes(components):
            visited_nodes = set()

            for component in components:
                if component not in visited_nodes:
                    visited_nodes.add(component)
                    graph.add_node(component.name)

                node_attribs = {
                    'label': '{}\n({})'.format(component.name,
                                               component.__class__.__name__),
                    'description': (component.__class__.__doc__ or '')
                }

                graph.node[component.name].update(node_attribs)

        def build_edges(components, visited_nodes=None):
            if visited_nodes is None:
                visited_nodes = set()

            if len(components) > 0:
                next_components = set()

                for component in components:
                    if component in visited_nodes:
                        continue

                    for output in component.outputs:
                        if output.is_connected():
                            next_components.add(output.target_port.component)

                            edge_attribs = {
                                'label': '{} -> {}'.format(
                                    output.name, output.target_port.name),
                                'description': (output.description or '')
                            }

                            graph.add_edge(component.name,
                                           output.target_port.component.name,
                                           edge_attribs)

                    visited_nodes.add(component)

                build_edges(next_components, visited_nodes)

        build_nodes(self.components)
        build_edges(self.components)

        self.log.debug('Writing %s to "%s"...' % (self, file_path))
        nx.write_graphml(graph, file_path)
