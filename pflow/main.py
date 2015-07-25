import sys
import logging

import argparse

from . import graph
from .runtime import GeventRuntime
from .components import Graph, Repeat, RandomNumberGenerator, ConsoleLineWriter, Multiply

log = logging.getLogger(__name__)


class SuperAwesomeDemoGraph(Graph):
    def define(self):
        gen1 = RandomNumberGenerator('GEN_1')
        gen2 = RandomNumberGenerator('GEN_2')
        mul = Multiply('MUL_1')
        repeater = Repeat('RPT_1')
        logger = ConsoleLineWriter('LOG_1')
        self.add_component(gen1, gen2, mul, repeater, logger)

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

    rt = GeventRuntime()
    rt.execute_graph(g)


if __name__ == '__main__':
    main()
