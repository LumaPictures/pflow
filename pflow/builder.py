"""
Utility module for constructing graphs in a similar fashion to the FBP DSL
"""

import contextlib
from .states import ComponentState


class IIP(object):
    """
    Place-holder for an Initial Information Packet
    """
    def __init__(self, value):
        self.value = value


def get_vertex(g):

    class Vertex(object):
        graph = g

        def __init__(self, *args):
            assert len(args) in [2, 3]
            self.args = args

        def inport(self):
            comp = self.args[1]
            if isinstance(comp, basestring):
                comp = self.graph.get_component(comp)
            return comp.inputs[self.args[0]]

        def outport(self):
            comp = self.args[-2]
            if isinstance(comp, basestring):
                comp = self.graph.get_component(comp)
            return comp.outputs[self.args[-1]]

        def __rshift__(self, other):
            self.graph.connect(self.outport(), other.inport())
            return other

        def __rrshift__(self, other):
            self.graph.set_initial_packet(self.inport(), other.value)
            return other

    return Vertex


@contextlib.contextmanager
def build_graph(*args, **kwargs):
    """
    Context manager for initializing a graph using a style similar to FBP DSL.
    """
    kwargs['initialize'] = False
    g = Graph(*args, **kwargs)
    yield get_vertex(g)
    g.state = ComponentState.INITIALIZED


if __name__ == '__main__':
    from .core import Graph
    from pflow.components import *
    with build_graph('foo') as V:
        limit = 3
        IIP(42) >> V('SEED', RandomNumberGenerator('GEN_1'))
        IIP(limit) >> V('LIMIT', 'GEN_1')
        IIP(limit) >> V('LIMIT', RandomNumberGenerator('GEN_2'))
        V('GEN_1', 'OUT') >> V('IN', Repeat('RPT_1'), 'OUT') >> \
            V('X', Multiply('MUL_1'))
        V('GEN_2', 'OUT') >> V('IN', Sleep('SLEEP_1'), 'OUT') >> \
            V('Y', 'MUL_1', 'OUT') >> V('IN', ConsoleLineWriter('LOG_1'))
        IIP(5) >> V('DELAY', 'SLEEP_1')
    print V.graph
