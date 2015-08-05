from pflow.components import *


class ProcessSpawningLogger(Graph):
    def initialize(self):
        tail_1 = FileTailReader('TAIL_1')
        self.set_initial_packet(tail_1.inputs['PATH'], '/var/log/system.log')

        filter_1 = RegexFilter('FILTER_1')
        self.set_initial_packet(filter_1.inputs['REGEX'],
                                r' (USER|DEAD)_PROCESS: ')

        self.connect(tail_1.outputs['OUT'], filter_1.inputs['IN'])

        self.connect(filter_1.outputs['OUT'],
                     ConsoleLineWriter('LOG_1').inputs['IN'])
