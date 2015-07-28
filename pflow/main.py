import sys
import logging

import argparse

from . import graph
from .graph import InitialPacketGenerator, Component
from .runtime import SingleThreadedRuntime
from .port import InputPort, OutputPort
from .components import Graph, Repeat, RandomNumberGenerator, ConsoleLineWriter, Multiply, Sleep

log = logging.getLogger(__name__)


class HypeMachinePopularTracksReader(Component):
    def initialize(self):
        self.inputs.add(InputPort('API_KEY', type_=str))
        self.inputs.add(InputPort('COUNT', optional=True, type_=int))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import requests

        api_key = self.inputs['API_KEY'].receive().value

        count_packet = self.inputs['COUNT'].receive()
        if count_packet is not None:
            count = count_packet.value
        else:
            count = 10

        response = requests.get('https://api.hypem.com/v2/tracks?sort=latest&key=%s&count=%d' %
                                (api_key, count))
        tracks = response.json()

        for track in tracks:
            track_packet = self.create_packet(track)
            self.outputs['OUT'].send(track_packet)
            self.suspend()


class HypeMachineTrackStringifier(Component):
    def initialize(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        track_packet = self.inputs['IN'].receive()
        track = track_packet.value

        if not (track['artist'] and track['title']):
            self.drop(track_packet)
        else:
            transformed = '%(artist)s - %(title)s' % track
            self.outputs['OUT'].send(self.create_packet(transformed))


class HypeMachineGraph(Graph):
    def initialize(self):
        '''
        'swagger' -> API_KEY HYPE_1(HypeMachinePopularTracksReader)
        '50' -> COUNT HYPE_1 OUT -> IN STR_1(HypeMachineTrackStringifier)
        STR_1 OUT -> IN LOG_1(ConsoleLineWriter)
        '''
        api_key_iip = InitialPacketGenerator('swagger')
        count_iip = InitialPacketGenerator(50)

        hype_1 = HypeMachinePopularTracksReader('HYPE_1')
        api_key_iip.connect(hype_1.inputs['API_KEY'])
        count_iip.connect(hype_1.inputs['COUNT'])

        str_1 = HypeMachineTrackStringifier('STR_1')
        log_1 = ConsoleLineWriter('LOG_1')

        hype_1.outputs['OUT'].connect(str_1.inputs['IN'])
        str_1.outputs['OUT'].connect(log_1.inputs['IN'])

        self.add_component(api_key_iip, count_iip, hype_1, str_1, log_1)


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
    logging.basicConfig(level=logging.INFO)
    # argp = argparse.ArgumentParser(description='pflow')
    # args = argp.parse_args()

    log.info('Initializing graph...')

    #g = SuperAwesomeDemoGraph('AWESOME_1')
    g = HypeMachineGraph('HYPE_1')
    g.write_graphml('demo.graphml')

    rt = SingleThreadedRuntime()
    rt.execute_graph(g)


if __name__ == '__main__':
    main()
