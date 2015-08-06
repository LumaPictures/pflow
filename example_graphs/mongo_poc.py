from pflow.components import *


class MongoPocGraph(Graph):
    def initialize(self):
        num_source = RandomNumberGenerator('NUM_SOURCE')
        self.set_initial_packet(num_source.inputs['LIMIT'], 200)

        binner = Binner('BINNER')
        self.set_initial_packet(binner.inputs['MAX_SIZE'], 300)
        self.connect(num_source.outputs['OUT'], binner.inputs['IN'])

        mongo_writer = MongoCollectionWriter('MONGO_WRITER')
        mongo_writer.inputs['IN'].max_queue_size = 1
        self.set_initial_packet(mongo_writer.inputs['MONGO_URI'], 'mongodb://localhost:27017')
        self.set_initial_packet(mongo_writer.inputs['MONGO_DATABASE'], 'mongo_poc')
        self.set_initial_packet(mongo_writer.inputs['MONGO_COLLECTION'], 'nums')
        self.set_initial_packet(mongo_writer.inputs['DELETE_COLLECTION'], True)

        console_writer = ConsoleLineWriter('CONSOLE_WRITER')
        #self.set_initial_packet(console_writer.inputs['SILENCE'], True)

        splitter = Splitter('SPLITTER')
        splitter.inputs['IN'].max_queue_size = 1
        self.connect(binner.outputs['OUT'], splitter.inputs['IN'])
        self.connect(splitter.outputs['OUT_A'], mongo_writer.inputs['IN'])
        self.connect(splitter.outputs['OUT_B'], console_writer.inputs['IN'])
