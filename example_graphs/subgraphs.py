from pflow.components import *


class LogTap(Graph):
    """
    Taps an input stream by receiving inputs from IN, sending them
    to the console log, and forwarding them to OUT.
    """
    def initialize(self):
        split = Split('SPLIT')
        self.inputs.export('IN', split.inputs['IN'])
        self.connect(split.outputs['OUT_A'], ConsoleLineWriter('LOG').inputs['IN'])
        self.outputs.export('OUT', split.outputs['OUT_B'])


class SubGraphExample(Graph):
    def initialize(self):
        gen = RandomNumberGenerator('GEN')
        self.set_initial_packet(gen.inputs['LIMIT'], 3)

        log_2 = ConsoleLineWriter('LOG_2')

        tap_1 = LogTap('TAP_1')
        tap_2 = LogTap('TAP_2')

        self.connect(gen.outputs['OUT'], tap_1.inputs['IN'])
        self.connect(tap_1.outputs['OUT'], tap_2.inputs['IN'])
        self.connect(tap_2.outputs['OUT'], log_2.inputs['IN'])

        #self.inputs.export('IN', tap.inputs['IN'])
        #self.outputs.export('OUT', tap.outputs['OUT'])
