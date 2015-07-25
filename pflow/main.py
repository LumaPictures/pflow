import sys
import logging

import argparse

from . import graph
from .runtime import GeventRuntime
from .components import Graph, Repeat, RandomNumberGenerator, ConsoleLineWriter

log = logging.getLogger(__name__)


class RandomNumberLogger(Graph):
    def define(self):
        genner = RandomNumberGenerator('GEN_1')
        repeater = Repeat('REPEAT_1')
        logger = ConsoleLineWriter('LOG_1')
        self.add_component(genner, repeater, logger)

        genner.outputs['OUT'].connect(repeater.inputs['IN'])
        repeater.outputs['OUT'].connect(logger.inputs['IN'])


def main():
    logging.basicConfig(level=logging.DEBUG)
    argp = argparse.ArgumentParser(description='pflow')
    args = argp.parse_args()

    log.info('Initializing graph...')

    g = RandomNumberLogger('RND_NUM_LOG')
    g.write_graphml('demo.graphml')

    rt = GeventRuntime()
    rt.execute_graph(g)


if __name__ == '__main__':
    main()
