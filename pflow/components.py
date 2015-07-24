from .graph import Component, \
    InputPort, OutputPort, \
    ArrayInputPort, ArrayOutputPort


class Repeat(Component):
    '''
    Repeats inputs from IN to OUT
    '''
    def define(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        packet = self.inputs.IN.receive()
        self.outputs.OUT.send(packet)


class Sleep(Component):
    '''
    Repeater that sleeps for DELAY seconds before
    repeating inputs from IN to OUT.
    '''
    def define(self):
        self.inputs.add(InputPort('IN'))
        self.inputs.add(InputPort('DELAY', type_=int))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import time

        packet = self.inputs.IN.receive()
        time.sleep(self.inputs.DELAY.value)
        self.outputs.OUT.send(packet)


class Split(Component):
    '''
    Splits inputs from IN to OUT[]
    '''
    def define(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(ArrayOutputPort('OUT', 10))

    def run(self):
        packet = self.inputs.IN.receive()
        for outp in self.outputs.OUT:
            outp.send(packet)


class Concat(Component):
    '''
    Concatenate inputs from IN[] into OUT
    '''
    def define(self):
        self.inputs.add(ArrayInputPort('IN', 10))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        for inp in self.inputs.IN:
            packet = inp.read()
            self.outports.OUT.send(packet)


class ConsoleLineWriter(Component):
    '''
    Write everything from IN to the console.
    This component is a sink.
    '''
    def define(self):
        self.inputs.add(InputPort('IN'))

    def run(self):
        packet = self.inputs.IN.receive()
        print packet.value
