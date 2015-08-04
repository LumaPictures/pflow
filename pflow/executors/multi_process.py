import sys
import collections
import time
import multiprocessing as mp
import traceback
import json
from abc import ABCMeta, abstractmethod

try:
    import cStringIO as StringIO
except ImportError:
    import StringIO

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

from .base import GraphExecutor, NoopSerializer
from ..core import ComponentState
from ..exc import GraphExecutorError
from ..port import Packet


class MultiProcessGraphExecutor(GraphExecutor):
    """
    Executes a graph in parallel using multiple processes, where each component is run in
    its own process.

    This runtime is useful for work that needs to take advantage of multicore execution
    and is best suited for components that may tend to be CPU bound.
    """
    def __init__(self, graph):
        super(MultiProcessGraphExecutor, self).__init__(graph)
        self._packet_serializer = NoopSerializer()
        self._in_queues = None
        self._out_queues = None
        self._running = False

    def _create_wrapped_runner(self, component):
        """
        Wraps the runner so that it shuts down all components when process raises
        an exception.

        :param component:
        :return:
        """
        run_loop = self._create_component_runner(component)

        def wrapped_runner(*args):
            import sys

            try:
                run_loop(*args)
            except Exception, ex:
                component.log.exception(ex)

                ex_tb = StringIO.StringIO()
                traceback.print_exc(file=ex_tb)
                # TODO: Pass ex_tb back to master process

                component.terminate()
                for c in self.graph.components:
                    c.terminate()

                raise ex

        return wrapped_runner

    def execute(self):
        self._running = True
        self.log.debug('Executing %s' % self.graph)

        self._in_queues = {}
        self._out_queues = {}

        # Create queues for all edges
        edges = set()
        for component in self.graph.components:
            for out_port in component.outputs:
                if out_port.is_connected():
                    edges.add(((component.name, out_port.name),
                               (out_port.target_port.component.name, out_port.target_port.name),
                               mp.Queue(maxsize=out_port.target_port.max_queue_size)))

        # Start all processes
        self.log.debug('Starting %d processes...' % len(self.graph.components))
        processes = []
        for component in self.graph.components:
            out_edges = filter(lambda edge: edge[0][0] == component.name, edges)
            in_edges  = filter(lambda edge: edge[1][0] == component.name, edges)

            in_queues = dict([(edge[1][1], edge[2]) for edge in in_edges])
            self._in_queues[component.name] = in_queues

            out_queues = dict([(edge[0][1], edge[2]) for edge in out_edges])
            self._out_queues[component.name] = out_queues

            process = mp.Process(target=self._create_wrapped_runner(component),
                                 args=(in_queues,    # in_queues
                                       out_queues),  # out_queues
                                 name=component.name)

            process.daemon = True
            process.start()
            processes.append(process)

        # Wait for all processes to terminate
        # TODO: Close ports on dead upstream processes
        self.log.debug('Waiting for process completion....')
        for process in processes:
            process.join()

        self._running = False
        self.log.debug('Finished graph execution')

    def is_running(self):
        return self._running

    def send_port(self, component, port_name, packet):
        self.log.debug('Sending packet to port %s.%s' % (component.name, port_name))

        q = self._get_outport_queue(component, port_name)
        component.state = ComponentState.SUSP_SEND
        q.put(self._packet_serializer.serialize(packet))
        component.state = ComponentState.ACTIVE

    def receive_port(self, component, port_name, timeout=None):
        # TODO: implement timeout
        if timeout is not None:
            raise NotImplementedError('timeout not implemented')

        self.log.debug('Receiving packet on port %s.%s' % (component.name, port_name))

        q = self._get_inport_queue(component, port_name)
        component.state = ComponentState.SUSP_RECV
        serialized_packet = q.get()
        component.state = ComponentState.ACTIVE

        self.log.debug('Packet received on %s.%s: %s' % (component.name, port_name, serialized_packet))
        packet = self._packet_serializer.deserialize(serialized_packet)

        return packet

    def _get_inport_queue(self, component, port_name):
        if hasattr(component, '_in_queues'):
            # Called from component process
            return component._in_queues[port_name]
        else:
            # Called from master process
            return self._in_queues[component.name][port_name]

    def _get_outport_queue(self, component, port_name):
        if hasattr(component, '_out_queues'):
            # Called from component process
            return component._out_queues[port_name]
        else:
            # Called from master process
            return self._out_queues[component.name][port_name]

    def close_input_port(self, component, port_name):
        self.log.debug('Closing input port %s.%s' % (component.name, port_name))

        q = self._get_inport_queue(component, port_name)
        q.close()

    def close_output_port(self, component, port_name):
        self.log.debug('Closing output port %s.%s' % (component.name, port_name))

        q = self._get_outport_queue(component, port_name)
        q.close()

    def terminate_thread(self, component):
        if not component.is_terminated:
            # This will cause the component loop to exit on the next iteration.
            component.state = ComponentState.TERMINATED

        self.log.debug('Closing %d inports for %s...' % (len(component.inputs), component))
        for in_port in component.inputs:
            if in_port.is_open():
                in_port.close()

    def suspend_thread(self, seconds=None):
        time.sleep(seconds or 0)
