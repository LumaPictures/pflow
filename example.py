#!/usr/bin/env python
import os
use_multi_process = (os.environ.get('PFLOW_MULTIPROCESS', '') == '1')

if use_multi_process:
    from pflow.runtimes.multi_process import MultiProcessGraphRuntime as GraphRuntimeImpl
else:
    # Need to load before logging
    from pflow.runtimes.single_process import SingleProcessGraphRuntime as GraphRuntimeImpl

import logging

from pflow.components import *

log = logging.getLogger(__name__)


class HypeMachinePopularTracksReader(Component):
    def initialize(self):
        self.inputs.add(InputPort('API_KEY', allowed_types=[str]),
                        InputPort('COUNT',
                                  optional=True,
                                  allowed_types=[int]))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import requests

        api_key = self.inputs['API_KEY'].receive()

        count = self.inputs['COUNT'].receive()
        if count is None:
            count = 10

        response = requests.get('https://api.hypem.com/v2/tracks?sort=popular&key=%s&count=%d' %
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
        """
        'swagger' -> API_KEY HYPE_1(HypeMachinePopularTracksReader)
        '50' -> COUNT HYPE_1 OUT -> IN STR_1(HypeMachineTrackStringifier)
        STR_1 OUT -> IN LOG_1(ConsoleLineWriter)
        """
        hype_1 = HypeMachinePopularTracksReader('HYPE_1')
        self.set_initial_packet(hype_1.inputs['API_KEY'], 'swagger')
        self.set_initial_packet(hype_1.inputs['COUNT'], 50)

        str_1 = HypeMachineTrackStringifier('STR_1')
        log_1 = ConsoleLineWriter('LOG_1')

        self.connect(hype_1.outputs['OUT'], str_1.inputs['IN'])
        self.connect(str_1.outputs['OUT'], log_1.inputs['IN'])


class ProcessSpawningLogger(Graph):
    def initialize(self):
        tail_1 = FileTailReader('TAIL_1')
        self.set_initial_packet(tail_1.inputs['PATH'], '/var/log/system.log')

        filter_1 = RegexFilter('FILTER_1')
        self.set_initial_packet(filter_1.inputs['REGEX'],
                                r' (USER|DEAD)_PROCESS: ')

        self.connect(tail_1.outputs['OUT'], filter_1.inputs['IN'])

        self.connect(filter_1.outputs['OUT'],
                     ConsoleLineWriter('LOG_1').inputs['IN'])


class SuperAwesomeDemoGraph(Graph):
    def initialize(self):
        """
        '42' -> SEED GEN_1(RandomNumberGenerator)
        '3' -> LIMIT GEN_1
        '3' -> LIMIT GEN_2(RandomNumberGenerator)
        GEN_1 OUT -> IN RPT_1(Repeat) -> X MUL_1(Multiply)
        GEN_2 OUT -> IN SLEEP_1(Sleep) OUT -> Y MUL_1 OUT -> IN LOG_1(ConsoleLineWriter)
        '5' -> DELAY SLEEP_1
        """
        gen_1 = RandomNumberGenerator('GEN_1')
        self.set_initial_packet(gen_1.inputs['SEED'], 42)
        self.set_initial_packet(gen_1.inputs['LIMIT'], 5)

        gen_2 = RandomNumberGenerator('GEN_2')
        self.set_initial_packet(gen_2.inputs['LIMIT'], 5)

        rpt_1 = Repeat('RPT_1')
        self.connect(gen_1.outputs['OUT'], rpt_1.inputs['IN'])

        mul_1 = Multiply('MUL_1')

        sleep_1 = Sleep('SLEEP_1')
        self.set_initial_packet(sleep_1.inputs['DELAY'], 1)
        self.connect(gen_2.outputs['OUT'], sleep_1.inputs['IN'])
        self.connect(sleep_1.outputs['OUT'], mul_1.inputs['Y'])

        log_1 = ConsoleLineWriter('LOG_1')
        self.connect(mul_1.outputs['OUT'], log_1.inputs['IN'])

        self.connect(rpt_1.outputs['OUT'], mul_1.inputs['X'])


def run_graph(graph):
    log.debug('Runtime is: %s' % GraphRuntimeImpl.__name__)

    graph.write_graphml(os.path.expanduser('~/%s.graphml' % graph.name))

    runtime = GraphRuntimeImpl(graph)
    runtime.execute()


def init_logger():
    # File logger
    logging.basicConfig(level=logging.DEBUG,
                        format='%(asctime)s | %(processName)-20s | %(levelname)-5s | %(name)s: %(message)s',
                        filename='example.log',
                        filemode='w')

    # Console logger
    console = logging.StreamHandler()
    console.setLevel(logging.INFO)
    console.setFormatter(logging.Formatter('%(processName)-20s | %(levelname)-5s | %(name)s: %(message)s'))
    logging.getLogger('').addHandler(console)

    # Set verbosity on packages
    logging.getLogger('requests').setLevel(logging.WARN)


def main():
    init_logger()

    #fbp_graph = Graph('FBP_1')
    #fbp_graph.load_fbp_file('./example/awesome.fbp')

    test_graphs = [
        SuperAwesomeDemoGraph('AWESOME_1'),
        PopularMusicGraph('MUSIC_1'),
        #fbp_graph,
        #ProcessSpawningLogger('PROCSPAWN_1')
    ]

    for graph in test_graphs:
        run_graph(graph)


if __name__ == '__main__':
    main()
