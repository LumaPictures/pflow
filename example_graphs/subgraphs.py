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
        self.outputs.export('OUT', split.outputs['OUT_A'])


class SubGraphExample(Graph):
    def initialize(self):
        gen = RandomNumberGenerator('GEN')
        self.set_initial_packet(gen.inputs['LIMIT'], 3)

        tap = LogTap('LOG_TAP')
        self.connect(gen.outputs['OUT'], tap.inputs['IN'])

        self.inputs.export('IN', tap.inputs['IN'])
        self.outputs.export('OUT', tap.outputs['OUT'])
