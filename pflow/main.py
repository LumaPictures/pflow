import sys
import logging

import argparse

from . import graph
from .graph import InitialPacketGenerator
from .runtime import SingleThreadedRuntime
from .components import Graph, Repeat, RandomNumberGenerator, ConsoleLineWriter, Multiply

log = logging.getLogger(__name__)


class SuperAwesomeDemoGraph(Graph):
    def initialize(self):
        seed = InitialPacketGenerator(42)
        limit1 = InitialPacketGenerator(3)
        limit2 = InitialPacketGenerator(3)

        gen1 = RandomNumberGenerator('GEN_1')
        seed.outputs['OUT'].connect(gen1.inputs['SEED'])
        limit1.outputs['OUT'].connect(gen1.inputs['LIMIT'])

        gen2 = RandomNumberGenerator('GEN_2')
        limit2.outputs['OUT'].connect(gen2.inputs['LIMIT'])

        mul = Multiply('MUL_1')
        repeater = Repeat('RPT_1')
        logger = ConsoleLineWriter('LOG_1')
        self.add_component(limit1, limit2, seed, gen1, gen2, mul, repeater, logger)

        gen1.outputs['OUT'].connect(repeater.inputs['IN'])
        repeater.outputs['OUT'].connect(mul.inputs['X'])
        gen2.outputs['OUT'].connect(mul.inputs['Y'])
        mul.outputs['OUT'].connect(logger.inputs['IN'])


def main():
    logging.basicConfig(level=logging.DEBUG)
    argp = argparse.ArgumentParser(description='pflow')
    args = argp.parse_args()

    log.info('Initializing graph...')

    g = SuperAwesomeDemoGraph('AWESOME_1')
    g.write_graphml('demo.graphml')

    rt = SingleThreadedRuntime()
    rt.execute_graph(g)


if __name__ == '__main__':
    main()
