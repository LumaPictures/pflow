import sys
import logging

import argparse

from . import graph, runtime
from .components import Repeat

log = logging.getLogger(__name__)


class MyGraph(graph.Graph):
    def define(self):
        self.add_component(Repeat('REPEAT_01'))


def main():
    logging.basicConfig(level=logging.DEBUG)
    argp = argparse.ArgumentParser(description='pflow')
    args = argp.parse_args()

    log.info('Initializing graph...')
    g = MyGraph('MY_GRAPH')
    g.run()


if __name__ == '__main__':
    main()
