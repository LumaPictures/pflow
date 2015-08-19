from pflow.components import *


class MongoPocGraph(Graph):
    def initialize(self):
        count = 15
        max_delay = 5

        num_source = RandomNumberGenerator('NUM_SOURCE')
        self.set_initial_packet(num_source.inputs['LIMIT'], count)

        delay_source = RandomNumberGenerator('DELAY_SOURCE')
        self.set_initial_packet(delay_source.inputs['LIMIT'], count)
        sleeper = DynamicSleep('SLEEPER')

        const_modulo = Constant('CONST_MOD')
        self.set_initial_packet(const_modulo.inputs['LIMIT'], count)
        self.set_initial_packet(const_modulo.inputs['VALUE'], max_delay)

        delay_modulo = Modulo('MOD')
        self.connect(delay_source.outputs['OUT'], delay_modulo.inputs['IN'])
        self.connect(const_modulo.outputs['OUT'], delay_modulo.inputs['MODULO'])
        self.connect(delay_modulo.outputs['OUT'], sleeper.inputs['DELAY'])

        binner = Binner('BINNER')
        self.set_initial_packet(binner.inputs['MAX_SIZE'], 300)
        self.set_initial_packet(binner.inputs['TIMEOUT'], 2)

        self.connect(num_source.outputs['OUT'], sleeper.inputs['IN'])
        self.connect(sleeper.outputs['OUT'], binner.inputs['IN'])
        # self.connect(num_source.outputs['OUT'], binner.inputs['IN'])

        mongo_writer = MongoCollectionWriter('MONGO_WRITER')
        # mongo_writer.inputs['IN'].max_queue_size = 1
        self.set_initial_packet(mongo_writer.inputs['MONGO_URI'], 'mongodb://localhost:27017')
        self.set_initial_packet(mongo_writer.inputs['MONGO_DATABASE'], 'mongo_poc')
        self.set_initial_packet(mongo_writer.inputs['MONGO_COLLECTION'], 'nums')
        self.set_initial_packet(mongo_writer.inputs['DELETE_COLLECTION'], True)

        console_writer = ConsoleLineWriter('CONSOLE_WRITER')
        #self.set_initial_packet(console_writer.inputs['SILENCE'], True)

        splitter = Split('SPLITTER')
        # splitter.inputs['IN'].max_queue_size = 1
        self.connect(binner.outputs['OUT'], splitter.inputs['IN'])
        self.connect(splitter.outputs['OUT_A'], mongo_writer.inputs['IN'])
        self.connect(splitter.outputs['OUT_B'], console_writer.inputs['IN'])
