import requests

from pflow.components import *


class HypeMachinePopularTracksReader(Component):
    def initialize(self):
        self.inputs.add_ports(InputPort('API_KEY',
                                        allowed_types=[str]),
                              InputPort('COUNT',
                                        optional=True,
                                        allowed_types=[int]))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        api_key = self.inputs['API_KEY'].receive()
        if api_key is EndOfStream:
            api_key = ''

        count = self.inputs['COUNT'].receive()
        if count is EndOfStream:
            count = 10

        response = requests.get('https://api.hypem.com/v2/tracks?sort=popular&key=%s&count=%d' %
                                (api_key, count))
        tracks = response.json()

        for track in tracks:
            self.outputs['OUT'].send(track)


class HypeMachineTrackStringifier(Component):
    def initialize(self):
        # self.inputs.add_ports(InputPort('IN', max_queue_size=1))  # Causes the upstream component to block in SUSP_SEND
        #                                                           # until this component can process the next packet.
        self.inputs.add_ports(InputPort('IN'))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        track = self.inputs['IN'].receive()

        if track is not EndOfStream and track['artist'] and track['title']:
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
