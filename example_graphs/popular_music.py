import requests

from pflow.components import *


class HypeTrackReader(Component):
    """
    Reads popular tracks from HypeMachine.
    """
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

        self.terminate()


class HypeTrackToDocumentTransformer(Component):
    """
    Converts HypeMachine tracks into an internal MongoDB document representation.
    """
    def initialize(self):
        # self.inputs.add_ports(InputPort('IN', max_queue_size=1))  # Causes the upstream component to block in SUSP_SEND
        #                                                           # until this component can process the next packet.
        self.inputs.add_ports(InputPort('IN'))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        track = self.inputs['IN'].receive()
        if track is EndOfStream:
            self.terminate()
            return
        elif track['artist'] and track['title']:
            transformed = {
                'label': '%(artist)s - %(title)s' % track,
                'foo': 'bar'
            }
            self.outputs['OUT'].send(transformed)


class PopularMusicGraph(Graph):
    def initialize(self):
        track_reader = HypeTrackReader('TRACK_READER')
        self.set_initial_packet(track_reader.inputs['API_KEY'], 'swagger')
        self.set_initial_packet(track_reader.inputs['COUNT'], 50)

        mongo_writer = MongoCollectionWriter('MONGO_WRITER')
        self.set_initial_packet(mongo_writer.inputs['MONGO_URI'], 'mongodb://localhost:27017')
        self.set_initial_packet(mongo_writer.inputs['MONGO_DATABASE'], 'popular_music')
        self.set_initial_packet(mongo_writer.inputs['MONGO_COLLECTION'], 'tracks')
        self.set_initial_packet(mongo_writer.inputs['DELETE_COLLECTION'], True)

        console_writer = ConsoleLineWriter('CONSOLE_WRITER')

        splitter = Splitter('SPLITTER')
        transform = HypeTrackToDocumentTransformer('TRANSFORM')
        title_extractor = DictValueExtractor('TITLE_EXTRACTOR')
        self.set_initial_packet(title_extractor.inputs['KEY'], 'label')

        self.connect(track_reader.outputs['OUT'], transform.inputs['IN'])
        self.connect(transform.outputs['OUT'], splitter.inputs['IN'])
        self.connect(splitter.outputs['OUT_A'], title_extractor.inputs['IN'])
        self.connect(title_extractor.outputs['OUT'], console_writer.inputs['IN'])
        self.connect(splitter.outputs['OUT_B'], mongo_writer.inputs['IN'])
