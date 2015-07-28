from .runtimes.single_process import SingleThreadedRuntime  # Need to load before logging

import os
import sys
import logging

from . import core
from .core import InitialPacketGenerator, Component
from .port import InputPort, OutputPort
from .components import *

log = logging.getLogger(__name__)


class HypeMachinePopularTracksReader(Component):
    def initialize(self):
        self.inputs.add(InputPort('API_KEY', type_=str),
                        InputPort('COUNT',
                                  optional=True,
                                  type_=int))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import requests

        api_key = self.inputs['API_KEY'].receive()

        count = self.inputs['COUNT'].receive()
        if count is None:
            count = 10

        response = requests.get('https://api.hypem.com/v2/tracks?sort=latest&key=%s&count=%d' %
                                (api_key, count))
        tracks = response.json()

        for track in tracks:
            self.outputs['OUT'].send(track)


class HypeMachineTrackStringifier(Component):
    def initialize(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        track = self.inputs['IN'].receive()

        if track['artist'] and track['title']:
            transformed = '%(artist)s - %(title)s' % track
            self.outputs['OUT'].send(transformed)


class PopularMusicGraph(Graph):
    def initialize(self):
        '''
        'swagger' -> API_KEY HYPE_1(HypeMachinePopularTracksReader)
        '50' -> COUNT HYPE_1 OUT -> IN STR_1(HypeMachineTrackStringifier)
        STR_1 OUT -> IN LOG_1(ConsoleLineWriter)
        '''
        api_key_iip = self.add_component(InitialPacketGenerator('swagger'))
        count_iip = self.add_component(InitialPacketGenerator(50))

        hype_1 = self.add_component(HypeMachinePopularTracksReader('HYPE_1'))
        api_key_iip.connect(hype_1.inputs['API_KEY'])
        count_iip.connect(hype_1.inputs['COUNT'])

        str_1 = self.add_component(HypeMachineTrackStringifier('STR_1'))
        log_1 = self.add_component(ConsoleLineWriter('LOG_1'))

        hype_1.outputs['OUT'].connect(str_1.inputs['IN'])
        str_1.outputs['OUT'].connect(log_1.inputs['IN'])


class ProcessSpawningLogger(Graph):
    def initialize(self):
        file_path_iip = self.add_component(InitialPacketGenerator('/var/log/system.log'))
        tail_1 = self.add_component(FileTailReader('TAIL_1'))
        file_path_iip.connect(tail_1.inputs['PATH'])

        filter_regex_iip = self.add_component(InitialPacketGenerator(r' (USER|DEAD)_PROCESS: '))
        filter_1 = self.add_component(RegexFilter('FILTER_1'))
        filter_regex_iip.connect(filter_1.inputs['REGEX'])
        tail_1.outputs['OUT'].connect(filter_1.inputs['IN'])

        log_1 = self.add_component(ConsoleLineWriter('LOG_1'))
        filter_1.outputs['OUT'].connect(log_1.inputs['IN'])


class SuperAwesomeDemoGraph(Graph):
    def initialize(self):
        '''
        '42' -> SEED GEN_1(RandomNumberGenerator)
        '3' -> LIMIT GEN_1
        '3' -> LIMIT GEN_2(RandomNumberGenerator)
        GEN_1 OUT -> IN RPT_1(Repeat) -> X MUL_1(Multiply)
        GEN_2 OUT -> IN SLEEP_1(Sleep) OUT -> Y MUL_1 OUT -> IN LOG_1(ConsoleLineWriter)
        '5' -> DELAY SLEEP_1
        '''
        seed_iip = self.add_component(InitialPacketGenerator(42))
        limit_iip_1 = self.add_component(InitialPacketGenerator(5))
        limit_iip_2 = self.add_component(InitialPacketGenerator(5))

        gen_1 = self.add_component(RandomNumberGenerator('GEN_1'))
        seed_iip.connect(gen_1.inputs['SEED'])
        limit_iip_1.connect(gen_1.inputs['LIMIT'])

        gen_2 = self.add_component(RandomNumberGenerator('GEN_2'))
        limit_iip_2.connect(gen_2.inputs['LIMIT'])

        rpt_1 = self.add_component(Repeat('RPT_1'))
        gen_1.outputs['OUT'].connect(rpt_1.inputs['IN'])

        mul_1 = self.add_component(Multiply('MUL_1'))

        sleep_iip_delay_1 = self.add_component(InitialPacketGenerator(1))
        sleep_1 = self.add_component(Sleep('SLEEP_1'))
        sleep_iip_delay_1.connect(sleep_1.inputs['DELAY'])
        gen_2.outputs['OUT'].connect(sleep_1.inputs['IN'])
        sleep_1.outputs['OUT'].connect(mul_1.inputs['Y'])

        log_1 = self.add_component(ConsoleLineWriter('LOG_1'))
        mul_1.outputs['OUT'].connect(log_1.inputs['IN'])

        rpt_1.outputs['OUT'].connect(mul_1.inputs['X'])

def run_graph(graph):
    log.info('Running graph: %s' % graph.name)
    graph.write_graphml(os.path.expanduser('~/%s.graphml' % graph.name))

    runtime = SingleThreadedRuntime()
    runtime.execute_graph(graph)


def main():
    logging.basicConfig(level=logging.INFO)
    logging.getLogger('requests').setLevel(logging.WARN)

    test_graphs = [
        SuperAwesomeDemoGraph('AWESOME_1'),
        PopularMusicGraph('MUSIC_1'),
        #ProcessSpawningLogger('PROCSPAWN_1')
    ]

    for graph in test_graphs:
        run_graph(graph)


if __name__ == '__main__':
    main()
