from .graph import Component, InputPort, OutputPort

class Repeat(Component):
    '''
    Repeats inputs from IN to OUT
    '''
    def define(self):
        self.inputs.add(InputPort('IN'))
        self.inputs.add(InputPort('CFG', optional=True))

        self.outputs.add(OutputPort('OUT'))

    def run(self):
        packet = self.inputs.IN.receive()
        self.outputs.out.send(packet)
