import logging

from .core import Graph, Component, ComponentState, \
    InputPort, OutputPort, \
    ArrayInputPort, ArrayOutputPort

from .port import EndOfStream


class Repeat(Component):
    """
    Repeats inputs from IN to OUT
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        packet = self.inputs['IN'].receive_packet()
        if packet is not EndOfStream:
            self.outputs['OUT'].send_packet(packet)


class Drop(Component):
    """
    Drops all inputs from IN.

    This component is a sink that acts like /dev/null
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'))

    def run(self):
        packet = self.inputs['IN'].receive_packet()
        if packet is not EndOfStream:
            self.drop(packet)


class Sleep(Component):
    """
    Repeater that sleeps for DELAY seconds before
    repeating inputs from IN to OUT.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'),
                              InputPort('DELAY',
                                        allowed_types=[int],
                                        description='Number of seconds to delay'))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        delay_value = self.inputs['DELAY'].receive()
        if delay_value is EndOfStream:
            delay_value = 0

        if delay_value == 0:
            self.log.warn('Using a %s component with 0 DELAY is the same as using Repeat' %
                          self.__class__.__name__)

        while not self.is_terminated:
            packet = self.inputs['IN'].receive_packet()
            if packet is EndOfStream:
                break

            self.log.debug('Sleeping for %d seconds...' % delay_value)
            self.suspend(delay_value)

            self.outputs['OUT'].send_packet(packet)


# class Split(Component):
#     """
#     Splits inputs from IN to OUT[]
#     """
#     def initialize(self):
#         self.inputs.add_ports(InputPort('IN'))
#         self.outputs.add_ports(ArrayOutputPort('OUT', 10))
#
#     def run(self):
#         packet = self.inputs['IN'].receive_packet()
#         if packet is not EndOfStream:
#             for outp in self.outputs['OUT']:
#                 outp.send_packet(packet)


class RegexFilter(Component):
    """
    Filters strings on IN against regex REGEX, sending matches to OUT
    and dropping non-matches.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN',
                                        allowed_types=[str],
                                        description='String to filter'),
                              InputPort('REGEX',
                                        allowed_types=[str],
                                        description='Regex to use for filtering'))
        self.outputs.add_ports(OutputPort('OUT',
                                          allowed_types=[str],
                                          description='String that matched filter'))

    def run(self):
        import re

        regex_value = self.inputs['REGEX'].receive()

        self.log.debug('Using regex filter: %s' % regex_value)
        pattern = re.compile(regex_value)

        while not self.is_terminated:
            packet = self.inputs['IN'].receive_packet()
            if packet is EndOfStream:
                break

            if pattern.search(packet.value) is not None:
                self.log.debug('Matched: "%s"' % packet.value)
                self.outputs['OUT'].send_packet(packet)
            else:
                self.log.debug('Dropped: "%s"' % packet.value)
                self.drop(packet)


# class Concat(Component):
#     """
#     Concatenates inputs from IN[] into OUT
#     """
#     def initialize(self):
#         self.inputs.add_ports(ArrayInputPort('IN', 10))
#         self.outputs.add_ports(OutputPort('OUT'))
#
#     def run(self):
#         for inp in self.inputs['IN']:
#             packet = inp.receive()
#             if packet is EndOfStream:
#                 break
#
#             self.outputs['OUT'].send_packet(packet)


class Multiply(Component):
    def initialize(self):
        self.inputs.add_ports(InputPort('X'),
                              InputPort('Y'))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        x = self.inputs['X'].receive()
        if x is EndOfStream:
            return

        y = self.inputs['Y'].receive()
        if y is EndOfStream:
            return

        result = int(x) * int(y)

        self.log.debug('Multiply %s * %s = %d' %
                       (x, y, result))

        self.outputs['OUT'].send(result)


class FileTailReader(Component):
    """
    Tails a file specified in input port PATH and follows it,
    emitting new lines that are added to output port OUT.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('PATH',
                                        description='File to tail',
                                        allowed_types=[str]))
        self.outputs.add_ports(OutputPort('OUT',
                                          description='Lines that are added to file'))

    def run(self):
        import sh

        file_path = self.inputs['PATH'].receive()
        if file_path is EndOfStream:
            return

        self.log.debug('Tailing file: %s' % file_path)

        for line in sh.tail('-f', file_path, _iter=True):
            stripped_line = line.rstrip()
            self.log.debug('Tailed line: %s' % stripped_line)

            self.outputs['OUT'].send(stripped_line)


class ConsoleLineWriter(Component):
    """
    Writes everything from IN to the console.

    This component is a sink.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'))

    def run(self):
        message = self.inputs['IN'].receive()
        if message is not EndOfStream:
            print message


class LogTap(Graph):
    """
    Taps an input stream by receiving inputs from IN, sending them
    to the console log, and forwarding them to OUT.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'))
        self.outputs.add_ports(OutputPort('OUT'))

        tap = Split('TAP')
        log = ConsoleLineWriter('LOG')

        self.connect(self.inputs['IN'], tap.inputs['IN'])
        self.connect(tap.outputs['OUT'][0], self.outputs['OUT'])
        self.connect(tap.outputs['OUT'][1], log.inputs['IN'])


class RandomNumberGenerator(Component):
    """
    Generates an sequence of random numbers, sending
    them all to the OUT port.

    This component is a generator.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('SEED',
                                        allowed_types=[int],
                                        optional=True,
                                        description='Seed value for PRNG'),
                              InputPort('LIMIT',
                                        allowed_types=[int],
                                        optional=True,
                                        description='Number of times to iterate (default: infinite)'))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        import random
        prng = random.Random()

        # Seed the PRNG
        seed_value = self.inputs['SEED'].receive()
        if seed_value is not EndOfStream:
            prng.seed(seed_value)

        limit_value = self.inputs['LIMIT'].receive()

        i = 0
        while True:
            random_value = prng.randint(1, 100)
            self.log.debug('Generated: %d' % random_value)

            packet = self.create_packet(random_value)
            self.outputs['OUT'].send_packet(packet)

            if limit_value is not EndOfStream:
                i += 1
                if i >= limit_value:
                    self.terminate()
                    break
