from pflow.components import *


class Simple(Component):
    def initialize(self):
        self.inputs.add('in')

    def run(self):
        while not self.is_terminated:
            print "connected %s" % self.inputs['in'].is_connected()
            print "open: %s" % self.inputs['in'].is_open()

            value = self.inputs['in'].receive()
            if value is EndOfStream:
                break

            print "value: %s" % value
            print "-"


class SimpleGraph(Graph):
    def initialize(self):
        self.set_initial_packet(Simple('Simple1').inputs['in'], 'foo')
