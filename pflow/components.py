import logging

from .graph import Graph, Component, ComponentState, \
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
        self.inputs.add(InputPort('IN'),
                        InputPort('DELAY',
                                  type_=int,
                                  description='Number of seconds to delay'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import time

        packet = self.inputs['IN'].receive()

        delay_value = self.inputs['DELAY'].receive_value()
        if delay_value is not None:
            self.log.debug('Sleeping for %d seconds...' % delay_value)
            self.state = ComponentState.SUSPENDED
            time.sleep(delay_value)
            # self.suspend(delay_value)
        else:
            self.log.warn('Using a %s component with no DELAY set is the same as using Repeat' %
                          self.__class__.__name__)

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


class RegexFilter(Component):
    '''
    Filters strings on IN against regex REGEX, sending matches to OUT.
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN',
                                  type_=str,
                                  description='String to filter'),
                        InputPort('REGEX',
                                  type_=str,
                                  description='Regex to use for filtering'))
        self.outputs.add(OutputPort('OUT',
                                    type_=str,
                                    description='String that matched filter'))

    def run(self):
        import re

        regex_value = self.inputs['REGEX'].receive_value()

        self.log.debug('Using regex filter: %s' % regex_value)
        pattern = re.compile(regex_value)

        while not self.is_terminated:
            packet = self.inputs['IN'].receive()

            if pattern.search(packet.value) is not None:
                self.log.debug('Matched: "%s"' % packet.value)
                self.outputs['OUT'].send(packet)
            else:
                self.log.debug('No match: "%s"' % packet.value)
                self.drop(packet)

            self.suspend()


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
        self.inputs.add(InputPort('X'),
                        InputPort('Y'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        x = self.inputs['X'].receive_value()
        y = self.inputs['Y'].receive_value()
        result = int(x) * int(y)

        self.log.debug('Multiply %s * %s = %d' %
                       (x, y, result))

        self.outputs['OUT'].send_value(result)


class FileTailReader(Component):
    '''
    Tails a file specified in input port PATH and follows it,
    emitting new lines that are added to output port OUT.
    '''
    def initialize(self):
        self.inputs.add(InputPort('PATH',
                                  description='File to tail',
                                  type_=str))
        self.outputs.add(OutputPort('OUT',
                                    description='Lines that are added to file'))

    def run(self):
        import sh

        file_path = self.inputs['PATH'].receive_value()
        self.log.debug('Tailing file: %s' % file_path)

        self.state = ComponentState.SUSPENDED
        for line in sh.tail('-f', file_path, _iter=True):
            stripped_line = line.rstrip()
            self.log.debug('Tailed line: %s' % stripped_line)

            self.outputs['OUT'].send_value(stripped_line)
            self.suspend()


class ConsoleLineWriter(Component):
    '''
    Writes everything from IN to the console.

    This component is a sink.
    '''
    def initialize(self):
        self.inputs.add(InputPort('IN'))

    def run(self):
        message = self.inputs['IN'].receive_value()
        print message


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
                                  description='Seed value for PRNG'),
                        InputPort('LIMIT',
                                  type_=int,
                                  optional=True,
                                  description='Number of times to iterate (default: infinite)'))
        self.outputs.add(OutputPort('OUT'))

    def run(self):
        import random
        prng = random.Random()

        # Seed the PRNG
        seed_value = self.inputs['SEED'].receive_value()
        if seed_value is not None:
            prng.seed(seed_value)

        limit_value = self.inputs['LIMIT'].receive_value()

        i = 0
        while True:
            random_value = prng.randint(1, 100)
            self.log.debug('Generated: %d' % random_value)

            packet = self.create_packet(random_value)
            self.outputs['OUT'].send(packet)
            self.suspend()

            if limit_value is not None:
                i += 1
                if i >= limit_value:
                    self.terminate()
                    break
