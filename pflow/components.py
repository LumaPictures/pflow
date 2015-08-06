import logging
import collections
import sys
import os
import copy

try:
    import queue  # 3.x
except ImportError:
    import Queue as queue  # 2.x

from .core import Graph, Component, ComponentState, \
    InputPort, OutputPort, \
    ArrayInputPort, ArrayOutputPort

from .port import EndOfStream, StartBracket, EndBracket, BracketPacket


class Repeat(Component):
    """
    Repeats inputs from IN to OUT
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'))
        self.outputs.add_ports(OutputPort('OUT'))

    def run(self):
        packet = self.inputs['IN'].receive_packet()
        if packet is EndOfStream:
            self.terminate()
        else:
            self.log.debug('Repeating: %s' % packet)
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
        if packet is EndOfStream:
            self.terminate()
        else:
            self.drop_packet(packet)


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
            self.log.warn('Using a %s component with no DELAY is the same as using Repeat' %
                          self.__class__.__name__)

        while not self.is_terminated:
            packet = self.inputs['IN'].receive_packet()
            if packet is EndOfStream:
                self.terminate()
                break

            self.log.debug('Sleeping for %d seconds...' % delay_value)
            self.suspend(delay_value)

            self.outputs['OUT'].send_packet(packet)


class Splitter(Component):
    """
    Splits inputs from IN to OUT_A and OUT_B.
    """
    def initialize(self):
        self.inputs.add('IN')
        self.outputs.add('OUT_A')
        self.outputs.add('OUT_B')

    def run(self):
        packet = self.inputs['IN'].receive_packet()
        if packet is EndOfStream:
            self.terminate()
            return

        out_a = self.outputs['OUT_A']
        out_b = self.outputs['OUT_B']

        if isinstance(packet, StartBracket):
            self.log.debug("Bracket open")
            out_a.start_bracket()
            out_b.start_bracket()
        elif isinstance(packet, EndBracket):
            self.log.debug("Bracket close")
            out_a.end_bracket()
            out_b.end_bracket()
        else:
            self.log.debug('Send: %s' % packet.value)
            out_a.send(packet.value)
            out_b.send(packet.value)

        self.drop_packet(packet)


# class Splitter(Component):
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


class Cons(Component):
    """
    Joins inputs A and B into tuples emitted to OUT.
    """
    def initialize(self):
        self.inputs.add('A')
        self.inputs.add('B')
        self.outputs.add_ports(OutputPort('OUT',
                                          description='Tuple stream'))

    def run(self):
        a = self.inputs['A'].receive()
        if a is EndOfStream:
            self.terminate()
            return

        b = self.inputs['B'].receive()
        if b is EndOfStream:
            self.terminate()
            return

        cons_val = (a, b)
        self.log.debug('Cons: ' + str(cons_val))

        self.outputs['OUT'].send(cons_val)


class Decons(Component):
    """
    Breaks apart tuples from input IN and emits values to OUT_A and OUT_B.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN',
                                        allowed_types=[tuple, list],
                                        description='Tuple stream'))
        self.outputs.add('OUT_A')
        self.outputs.add('OUT_B')

    def run(self):
        value = self.inputs['IN'].receive()
        if value is EndOfStream:
            self.terminate()
            return

        assert isinstance(value, (tuple, list))
        assert len(value) == 2

        a, b = value
        self.log.debug('Decons: %s, %s' % (a, b))

        self.outputs['OUT_A'].send(a)
        self.outputs['OUT_B'].send(b)


class DictValueExtractor(Component):
    """
    Filters a stream of dicts from input IN, extracting a stream of values
    (matching KEY) on OUT.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN',
                                        allowed_types=[collections.MutableMapping],
                                        description='Dictionaries to filter'),
                              InputPort('KEY',
                                        description='Key to extract values for'))
        self.outputs.add('OUT')

    def run(self):
        key = self.inputs['KEY'].receive()
        if key is EndOfStream:
            self.terminate()
            return

        while not self.is_terminated:
            d = self.inputs['IN'].receive()
            if d is EndOfStream:
                self.terminate()
                break

            assert isinstance(d, collections.MutableMapping)

            value = d.get(key)
            self.log.debug('Extracted: %s -> %s' % (key, value))

            self.outputs['OUT'].send(value)


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
                self.terminate()
                break

            if pattern.search(packet.value) is not None:
                self.log.debug('Matched: "%s"' % packet.value)
                self.outputs['OUT'].send_packet(packet)
            else:
                self.log.debug('Dropped: "%s"' % packet.value)
                self.drop_packet(packet)


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
        y = self.inputs['Y'].receive()

        if x is EndOfStream or y is EndOfStream:
            self.terminate()
            return

        result = int(x) * int(y)

        self.log.debug('Multiply %s * %s = %d' %
                       (x, y, result))

        self.outputs['OUT'].send(result)


class Binner(Component):
    def initialize(self):
        self.inputs.add_ports(InputPort('IN',
                                        description='Tuples of (value, size) or simply size'),
                              InputPort('MAX_SIZE',
                                        allowed_types=[int, float],
                                        description='Max size threshold before bin bracket gets sent downstream'),
                              InputPort('TIMEOUT',
                                        allowed_types=[int],
                                        optional=True,
                                        description='Number of seconds to wait for receive before closing bin'))
        self.outputs.add_ports(OutputPort('OUT',
                                          description='Stream of values, bracketed by size'))

    def run(self):
        max_size = self.inputs['MAX_SIZE'].receive()

        timeout = self.inputs['TIMEOUT'].receive()
        if timeout is EndOfStream:
            timeout = None
        else:
            self.log.debug('Will timeout receives after %d seconds' % timeout)

        outport = self.outputs['OUT']

        total = 0
        started = False

        while not self.is_terminated:
            value_tuple = self.inputs['IN'].receive(timeout=timeout)
            if value_tuple is EndOfStream:
                if started:
                    # End bin
                    self.log.debug('Ending bin')
                    outport.end_bracket()

                self.terminate()
                break
            elif isinstance(value_tuple, BracketPacket):
                # Ignore incoming brackets
                continue

            if isinstance(value_tuple, (list, tuple)):
                value, size = value_tuple
            else:
                value = size = value_tuple

            if not started:
                started = True
                # Start initial bin
                self.log.debug('Starting initial bin')
                outport.start_bracket()
                total = size
            elif size + total > max_size:
                total = size
                # Start new bin
                self.log.debug('Starting new bin (%d > %d)' % (total, max_size))
                outport.end_bracket()
                outport.start_bracket()
            else:
                total += size

            self.log.debug('Binned: %s' % value)
            outport.send(value)


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
            self.terminate()
            return

        self.log.debug('Tailing file: %s' % file_path)

        for line in sh.tail('-f', file_path, _iter=True):
            stripped_line = line.rstrip()
            self.log.debug('Tailed line: %s' % stripped_line)

            self.outputs['OUT'].send(stripped_line)

            if self.is_terminated:
                break


class ConsoleLineWriter(Component):
    """
    Writes everything from IN to the console.

    This component is a sink.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'),
                              InputPort('SILENCE',
                                        allowed_types=[bool],
                                        optional=True,
                                        description='Silence the console output? (useful for debugging)'))
        self.outputs.add_ports(OutputPort('OUT',
                                          optional=True))

    def run(self):
        silence = self.inputs['SILENCE'].receive()
        if silence is EndOfStream:
            silence = False

        depth = 0
        while not self.is_terminated:
            packet = self.inputs['IN'].receive_packet()
            if packet is EndOfStream:
                self.terminate()
                break
            else:
                if isinstance(packet, StartBracket):
                    if not silence:
                        self._write_line("[start group]")
                    depth += 1
                elif isinstance(packet, EndBracket):
                    if not silence:
                        self._write_line("[end group]")
                    depth -= 1
                elif not silence:
                    self._write('-' * depth)
                    self._write_line(packet.value)

                # Forward to output port
                if self.outputs['OUT'].is_connected():
                    self.outputs['OUT'].send_packet(packet)

                self.suspend()

    def _write(self, text):
        sys.stdout.write(text)

    def _write_line(self, line):
        self._write('%s%s' % (line, os.linesep))
        sys.stdout.flush()



class MongoCollectionWriter(Component):
    """
    Writes every record from IN to a MongoDB collection.
    """
    def initialize(self):
        self.inputs.add_ports(InputPort('IN'),
                              InputPort('MONGO_URI',
                                        allowed_types=[str],
                                        description='URI of the MongoDB server to connect to'),
                              InputPort('MONGO_DATABASE',
                                        allowed_types=[str],
                                        description='Name of the database to write to'),
                              InputPort('MONGO_COLLECTION',
                                        allowed_types=[str],
                                        description='Name of the collection to write to'),
                              InputPort('DELETE_COLLECTION',
                                        allowed_types=[bool],
                                        optional=True,
                                        description='If True, delete all documents in collection before writing to it'))

    def run(self):
        import pymongo

        mongo_uri = self.inputs['MONGO_URI'].receive()
        self.log.debug('Connecting to mongodb server: %s' % mongo_uri)

        mongo_client = pymongo.MongoClient(host=mongo_uri)
        self.log.debug('Connected!')

        db_name = self.inputs['MONGO_DATABASE'].receive()
        collection_name = self.inputs['MONGO_COLLECTION'].receive()
        collection = mongo_client.get_database(db_name).get_collection(collection_name)

        delete_collection = self.inputs['DELETE_COLLECTION'].receive()
        if delete_collection is not EndOfStream and delete_collection == True:
            self.log.warn('Deleting collection: %s' % collection_name)
            collection.remove()

        bracket_depth = 0
        batch = []

        try:
            while not self.is_terminated:
                packet = self.inputs['IN'].receive_packet()
                if packet is EndOfStream:
                    self.terminate()
                    break

                elif isinstance(packet, StartBracket):
                    bracket_depth += 1
                    self.drop_packet(packet)

                elif isinstance(packet, EndBracket):
                    bracket_depth -= 1
                    self.drop_packet(packet)

                    if bracket_depth == 0:
                        # Do batch insert
                        self.log.debug('Batch inserting %d records...' % len(batch))
                        if len(batch) > 0:
                            collection.insert_many(batch)

                        batch = []

                else:
                    # Value
                    value = packet.value
                    self.drop_packet(packet)

                    if not isinstance(value, collections.MutableMapping):
                        # Mongo only accepts dict-like structures.
                        value = {'value': value}

                    if bracket_depth == 0:
                        # Do a direct insert if there is no bracket open.
                        self.log.debug('Immediate insert: %s' % value)
                        collection.insert_one(value)
                    else:
                        # If there is a bracket open, buffer the insert so that it can be flushed
                        # with a bulk insert when the bracket is closed.
                        batch.append(value)
                        self.log.debug('Delayed insert: %s (rows=%s)' % (value, batch))

        finally:
            mongo_client.close()


# class LogTap(Graph):
#     """
#     Taps an input stream by receiving inputs from IN, sending them
#     to the console log, and forwarding them to OUT.
#     """
#     def initialize(self):
#         self.inputs.add_ports(InputPort('IN'))
#         self.outputs.add_ports(OutputPort('OUT'))
#
#         tap = Split('TAP')
#         log = ConsoleLineWriter('LOG')
#
#         self.connect(self.inputs['IN'], tap.inputs['IN'])
#         self.connect(tap.outputs['OUT'][0], self.outputs['OUT'])
#         self.connect(tap.outputs['OUT'][1], log.inputs['IN'])


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
        if limit_value is EndOfStream:
            limit_value = None

        if limit_value is None or limit_value > 0:
            i = 1
            while not self.is_terminated:
                random_value = prng.randint(1, 100)
                self.log.debug('Generated: %d (%d/%s)' % (random_value, i, limit_value))

                packet = self.create_packet(random_value)
                self.outputs['OUT'].send_packet(packet)

                if limit_value is not None:
                    if i >= limit_value:
                        self.terminate()
                        break

                i += 1
