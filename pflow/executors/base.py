import json
from abc import ABCMeta, abstractmethod

from ..core import ComponentState
from .. import exc


class GraphExecutor(object):
    """
    Executors are responsible for running a single graph: starting processes, scheduling execution,
    and forwarding messages on Connections between Processes.
    """
    __metaclass__ = ABCMeta

    def __init__(self, graph):
        from ..core import Graph
        import logging

        if not isinstance(graph, Graph):
            raise ValueError('graph must be a Graph object')

        self.graph = graph
        self.log = logging.getLogger('%s.%s' % (self.__class__.__module__,
                                                self.__class__.__name__))

        # Wire up executor to all graph components
        for component in graph.components:
            component.executor = self

    def _create_component_runner(self, component):
        """
        Creates a run loop for a component thread.

        :param component: the component to create the runner for.
        :return: loop function that gets executed by the thread.
        """
        def component_loop(in_queues, out_queues):
            component._in_queues = in_queues
            component._out_queues = out_queues

            try:
                # Component should always be in a NOT_STARTED state when first running!
                if component.state != ComponentState.NOT_STARTED:
                    raise exc.ComponentStateError('%s state is %s, but expected NOT_STARTED' % (component,
                                                                                                component.state))

                component.state = ComponentState.ACTIVE
                while not component.is_terminated:

                    # Run the component
                    component.run()

                    if self.graph.is_upstream_terminated(component):
                        if component.state == ComponentState.DORMANT:
                            # Terminate when all upstream components have terminated and there's no more data to process.
                            self.log.debug('%s will be marked TERMINATED because it is DORMANT '
                                           'with a dead upstream ' % component)
                            component.terminate()
                        elif not (component.is_suspended or component.is_terminated):
                            self.log.debug('%s will be marked as DORMANT because is not waiting for data' % component)
                            component.state = ComponentState.DORMANT
                    else:
                        # Suspend execution until there's more data to process.
                        component.suspend()
                        if not component.is_terminated:
                            component.state = ComponentState.ACTIVE

            finally:
                component.destroy()

        return component_loop

    def _reset_components(self):
        for component in self.graph.components:
            for inport in component.inputs:
                self.close_input_port(component, inport.name)

            for outport in component.outputs:
                self.close_output_port(component, outport.name)

            component._state = ComponentState.NOT_STARTED
            component.executor = None
            # TODO: component.stack

    @abstractmethod
    def is_running(self):
        pass

    @abstractmethod
    def execute(self):
        """
        Executes the graph.
        """
        pass

    def stop(self):
        """
        Stops graph execution.
        """
        if not self.is_running():
            return

        self.log.debug('Stopping graph execution...')
        for component in self.graph.components:
            component.terminate()

        self._reset_components()

    @abstractmethod
    def send_port(self, component, port_name, packet):
        """
        Sends a packet on a component's output port.

        :param component: the component the packet is being sent from.
        :param port_name: the name of the component's output port.
        :param packet: the packet to send.
        """
        pass

    @abstractmethod
    def receive_port(self, component, port_name, timeout=None):
        """
        Receives a packet from a component's input port.

        :param component: the component the packet is being received for.
        :param port_name: the name of the component's input port.
        :param timeout: number of seconds to wait for the next packet before raising a exc.PortReceiveTimeout.
        :return: the received packet.
        """
        pass

    @abstractmethod
    def close_input_port(self, component, port_name):
        """
        Closes a component's input port.

        :param component: the component who's input port should be closed.
        :param port_name: the name of the component's input port to close.
        """
        pass

    @abstractmethod
    def close_output_port(self, component, port_name):
        """
        Closes a component's output port.

        :param component: the component who's output port should be closed.
        :param port_name: the name of the component's output port to close.
        """
        pass

    @abstractmethod
    def terminate_thread(self, component):
        """
        Terminate this thread, making it no longer process packets.
        """
        pass

    @abstractmethod
    def suspend_thread(self, seconds=None):
        """
        Suspend execution of this thread until the next packet arrives.
        """
        pass


class PacketSerializer(object):
    """
    Responsible for serializing/deserializing packet data.
    """
    __metaclass__ = ABCMeta

    @abstractmethod
    def serialize(self, packet):
        pass

    @abstractmethod
    def deserialize(self, serialized_packet):
        pass


class JsonPacketSerializer(PacketSerializer):
    """
    JSON serializer.
    """
    def serialize(self, packet):
        from ..port import Packet
        if not isinstance(packet, Packet):
            raise ValueError('packet must be a Packet')

        # TODO: handle dates as iso8601
        return json.dumps(packet.value)

    def deserialize(self, serialized_packet):
        from ..port import Packet

        packet_value = json.loads(serialized_packet)
        return Packet(packet_value)


class NoopSerializer(PacketSerializer):
    """
    A serializer that basically does nothing.
    """
    def serialize(self, packet):
        from ..port import Packet
        if not isinstance(packet, Packet):
            raise ValueError('packet must be a Packet')

        return packet.value

    def deserialize(self, serialized_packet):
        from ..port import Packet

        return Packet(serialized_packet)
