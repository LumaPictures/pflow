import logging

from .graph import Graph, Component, \
    InputPort, OutputPort, \
    ArrayInputPort, ArrayOutputPort

log = logging.getLogger(__name__)


class Repeat(Component):
    '''
    Repeats inputs from IN to OUT
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        packet = self.inputs['IN'].receive()
        self.outputs['OUT'].send(packet)


class Drop(Component):
    '''
    Drops all inputs from IN.

    This component is a sink that acts like /dev/null
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN'))

    def run(self):
        packet = self.inputs['IN'].receive()
        self.drop(packet)


class Sleep(Component):
    '''
    Repeater that sleeps for DELAY seconds before
    repeating inputs from IN to OUT.
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN'))
        self.inputs.add(InputPort('DELAY',
                                  type_=int,
                                  description='Number of seconds to delay'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import time

        packet = self.inputs['IN'].receive()
        time.sleep(self.inputs['DELAY'].value)
        self.outputs['OUT'].send(packet)


class Split(Component):
    '''
    Splits inputs from IN to OUT[]
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(ArrayOutputPort('OUT', 10))

    def run(self):
        packet = self.inputs['IN'].receive()
        for outp in self.outputs['OUT']:
            outp.send(packet)


class Concat(Component):
    '''
    Concatenates inputs from IN[] into OUT
    '''
    def initialize(self):
        self.inputs.add(ArrayInputPort('IN', 10))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        for inp in self.inputs['IN']:
            packet = inp.read()
            self.outputs['OUT'].send(packet)


class Multiply(Component):
    def initialize(self):
        self.inputs.add(InputPort('X'))
        self.inputs.add(InputPort('Y'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        x_packet = self.inputs['X'].receive()
        y_packet = self.inputs['Y'].receive()
        result_packet = self.create_packet(int(x_packet.value) * int(y_packet.value))

        log.debug('%s: Multiply %s * %s = %d' % (self.name,
                                                 x_packet.value,
                                                 y_packet.value,
                                                 result_packet.value))

        self.outputs['OUT'].send(result_packet)


class ConsoleLineWriter(Component):
    '''
    Writes everything from IN to the console.

    This component is a sink.
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN'))

    def run(self):
        packet = self.inputs['IN'].receive()
        print packet.value


class LogTap(Graph):
    '''
    Taps an input stream by receiving inputs from IN, sending them
    to the console log, and forwarding them to OUT.
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN'))
        self.outputs.add(OutputPort('OUT'))

        tapper = Split('TAP')
        logger = ConsoleLineWriter('LOG')
        self.add_component(tapper, logger)

        # Wire shit up
        self.inputs['IN'].connect(tapper.inputs['IN'])
        tapper.outputs['OUT'][0].connect(self.outputs['OUT'])
        tapper.outputs['OUT'][1].connect(logger.inputs['IN'])


class RandomNumberGenerator(Component):
    '''
    Generates an sequence of random numbers, sending
    them all to the OUT port.

    This component is a generator.
    '''
    def initialize(self):
        self.inputs.add(InputPort('SEED',
                                  type_=int,
                                  optional=True,
                                  description='Seed value for PRNG'))
        self.inputs.add(InputPort('LIMIT',
                                  type_=int,
                                  optional=True,
                                  description='Number of times to iterate (default: infinite)'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import random
        prng = random.Random()

        # Seed the PRNG
        seed_packet = self.inputs['SEED'].receive()
        if seed_packet is not None:
            prng.seed(seed_packet.value)

        limit_packet = self.inputs['LIMIT'].receive()
        if limit_packet is not None:
            limit = limit_packet.value
        else:
            limit = None

        i = 0
        while True:
            random_value = prng.randint(1, 100)
            log.debug('%s: Generated %d' % (self.name, random_value))

            packet = self.create_packet(random_value)
            self.outputs['OUT'].send(packet)
            self.suspend()

            if limit is not None:
                i += 1
                if i >= limit:
                    self.terminate()
                    break
