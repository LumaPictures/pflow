from pflow.components import *


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
        limit = 3

        gen_1 = RandomNumberGenerator('GEN_1')
        self.set_initial_packet(gen_1.inputs['SEED'], 42)
        self.set_initial_packet(gen_1.inputs['LIMIT'], limit)

        gen_2 = RandomNumberGenerator('GEN_2')
        self.set_initial_packet(gen_2.inputs['LIMIT'], limit)

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
