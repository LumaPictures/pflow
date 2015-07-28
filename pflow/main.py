import sys
import logging

import argparse

from . import graph
from .graph import InitialPacketGenerator
from .runtime import SingleThreadedRuntime
from .components import Graph, Repeat, RandomNumberGenerator, ConsoleLineWriter, Multiply, Sleep

log = logging.getLogger(__name__)


class SuperAwesomeDemoGraph(Graph):
    def initialize(self):
        '''
        42 -> SEED GEN_1(RandomNumberGenerator)
        3 -> LIMIT GEN_1
        3 -> LIMIT GEN_2(RandomNumberGenerator)
        GEN_1 OUT -> IN RPT_1(Repeat) -> X MUL_1(Multiply)
        GEN_2 OUT -> IN SLEEP_1(Sleep) OUT -> Y MUL_1 OUT -> IN LOG_1(ConsoleLineWriter)
        5 -> DELAY SLEEP_1
        '''
        seed_iip = InitialPacketGenerator(42)
        limit_iip_1 = InitialPacketGenerator(3)
        limit_iip_2 = InitialPacketGenerator(3)

        gen_1 = RandomNumberGenerator('GEN_1')
        seed_iip.connect(gen_1.inputs['SEED'])
        limit_iip_1.connect(gen_1.inputs['LIMIT'])

        gen_2 = RandomNumberGenerator('GEN_2')
        limit_iip_2.connect(gen_2.inputs['LIMIT'])

        rpt_1 = Repeat('RPT_1')
        gen_1.outputs['OUT'].connect(rpt_1.inputs['IN'])

        mul_1 = Multiply('MUL_1')

        sleep_iip_delay_1 = InitialPacketGenerator(5)
        sleep_1 = Sleep('SLEEP_1')
        sleep_iip_delay_1.connect(sleep_1.inputs['DELAY'])
        gen_2.outputs['OUT'].connect(sleep_1.inputs['IN'])
        sleep_1.outputs['OUT'].connect(mul_1.inputs['Y'])

        log_1 = ConsoleLineWriter('LOG_1')
        mul_1.outputs['OUT'].connect(log_1.inputs['IN'])

        rpt_1.outputs['OUT'].connect(mul_1.inputs['X'])

        self.add_component(limit_iip_1,
                           limit_iip_2,
                           seed_iip,
                           gen_1,
                           gen_2,
                           sleep_1,
                           sleep_iip_delay_1,
                           mul_1,
                           rpt_1,
                           log_1)


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
