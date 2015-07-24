from .graph import Component, InputPort, OutputPort, ArrayInputPort, ArrayOutputPort


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
        self.outputs.OUT.send(packet)


class Split(Component):
    '''
    Splits inputs from IN to OUT[]
    '''
    def define(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(ArrayOutputPort('OUT'))

    def run(self):
        packet = self.inputs.IN.receive()
        for outp in self.outputs.OUT:
            outp.send(packet)


class Concat(Component):
    '''
    Concatenate inputs from IN[] into OUT
    '''
    def define(self):
        self.inputs.add(ArrayInputPort('IN'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        for inp in self.inputs.IN:
            packet = inp.read()
            outp.send(packet)
