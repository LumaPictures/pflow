import collections
import time
import multiprocessing as mp

# try:
#     import queue  # 3.x
# except ImportError:
#     import Queue as queue  # 2.x

from .base import Runtime


class MultiProcessRuntime(Runtime):
    """
    Executes a graph in parallel using multiple processes, where each component is run in
    its own process.

    This runtime is useful for work that needs to take advantage of multicore execution
    and is best suited for components that may tend to be CPU bound.
    """
    def __init__(self):
        super(MultiProcessRuntime, self).__init__()
        self._recv_queues = collections.defaultdict(mp.Queue)

    def execute_graph(self, graph):
        self.log.debug('Executing graph %s' % graph)
        self.inject_runtime(graph)

        processes = dict([(mp.Process(target=self.create_component_runner(c),
                                      name='Component: %s' % c.name), c)
                          for c in graph.components])

        # Start all processes
        self.log.debug('Starting %d processes...' % len(processes))
        for process in processes:
            process.daemon = True
            process.start()

        # TODO: Handle exceptions raised in threads

        # Wait for all processes to terminate
        self.log.debug('Waiting for process completion....')
        for process in processes:
            process.join()

        self.log.debug('Finished graph execution')

    def send(self, packet, dest_port):
        #     q = self._recv_queues[dest_port]
        #     self.log.debug('Sending packet to %s' % dest_port)
        #     q.put(packet)
        raise NotImplementedError

    def receive(self, source_port):
        #     q = self._recv_queues[source_port]
        #     component = source_port.component
        #     while True:
        #         try:
        #             packet = q.get(block=False)
        #             self.log.debug('%s received packet on %s' % (component, source_port))
        #             component.state = ComponentState.ACTIVE
        #             return packet
        #         except queue.Empty:
        #             if self.is_upstream_terminated(component):
        #                 # No more data left to receive_packet and upstream has terminated.
        #                 component.terminate()
        #             else:
        #                 self.log.debug('%s is waiting for packet on %s' % (component, source_port))
        #                 component.suspend()
        raise NotImplementedError

    def port_has_data(self, port):
        #     return (port in self._recv_queues and
        #             not self._recv_queues[port].empty())
        raise NotImplementedError

    def clear_port(self, port):
        #     if port in self._recv_queues:
        #         del self._recv_queues[port]
        raise NotImplementedError

    def terminate_thread(self, seconds=None):
        # This is a no-op since the component will set its state to TERMINATED,
        # which effectively exits the run loop.
        pass

    def suspend_thread(self, seconds=None):
        # TODO: Sleep until queue has data
        time.sleep(1)
