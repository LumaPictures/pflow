import collections
import time
import multiprocessing as mp
import json
from abc import ABCMeta, abstractmethod

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

from .base import GraphRuntime, NoopSerializer
from ..core import ComponentState
from ..exc import GraphRuntimeError
from ..port import Packet


class MultiProcessGraphRuntime(GraphRuntime):
    """
    Executes a graph in parallel using multiple processes, where each component is run in
    its own process.

    This runtime is useful for work that needs to take advantage of multicore execution
    and is best suited for components that may tend to be CPU bound.
    """
    def __init__(self, graph):
        super(MultiProcessGraphRuntime, self).__init__(graph)
        self._packet_serializer = NoopSerializer()

    def execute(self):
        self.log.debug('Executing graph %s' % self.graph)

        edges = set()
        for component in self.graph.components:
            for out_port in component.outputs:
                if out_port.is_connected():
                    edges.add(((component.name, out_port.name),
                               (out_port.target_port.component.name, out_port.target_port.name),
                               mp.Queue()))

        processes = {}
        for component in self.graph.components:
            out_edges = filter(lambda edge: edge[0][0] == component.name, edges)
            in_edges =  filter(lambda edge: edge[1][0] == component.name, edges)
            process = mp.Process(target=self.create_component_runner(component),
                                 args=(dict([(edge[1][1], edge[2]) for edge in in_edges]),    # in_queues
                                       dict([(edge[0][1], edge[2]) for edge in out_edges])),  # out_queues
                                 name=component.name)
            processes[process] = component

        # Start all processes
        self.log.debug('Starting %d processes...' % len(processes))
        for process in processes.keys():
            process.daemon = True
            process.start()

        # Wait for all processes to terminate
        # TODO: Close ports on dead upstream processes
        self.log.debug('Waiting for process completion....')
        for process in processes:
            process.join()

        self.log.debug('Finished graph execution')

    def send_port(self, component, port_name, packet):
        self.log.debug('Sending packet to port %s.%s' % (component.name, port_name))
        q = component._out_queues[port_name]
        q.put(self._packet_serializer.serialize(packet))

    def receive_port(self, component, port_name):
        self.log.debug('Receiving packet on port %s.%s' % (component.name, port_name))
        q = component._in_queues[port_name]

        if self.is_upstream_terminated(component):
            self.log.warn('Port %s.%s has no more data' % (component.name, port_name))
            component.terminate()

        serialized_packet = q.get()

        self.log.debug('Packet received on %s.%s: %s' % (component.name, port_name, serialized_packet))
        packet = self._packet_serializer.deserialize(serialized_packet)

        return packet

    def close_input_port(self, component, port_name):
        if port_name in component._in_queues:
            q = component._in_queues[port_name]
            q.close()

    def close_output_port(self, component, port_name):
        if port_name in component._out_queues:
            q = component._out_queues[port_name]
            q.close()

    def terminate_thread(self, component):
        if not component.is_terminated:
            # This will cause the component loop to exit.
            component.state = ComponentState.TERMINATED

    def suspend_thread(self, seconds=None):
        # TODO: Sleep until queue has data
        time.sleep(1)
