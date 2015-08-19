from pflow.components import *


class Simple(Component):
    def initialize(self):
        self.inputs.add('in')

    def run(self):
        while self.is_alive():
            print "connected %s" % self.inputs['in'].is_connected()
            print "open: %s" % self.inputs['in'].is_open()

            value = self.inputs['in'].receive()
            if value is EndOfStream:
                self.terminate()
                break

            print "value: %s" % value
            print "-"


class SimpleGraph(Graph):
    def initialize(self):
        self.set_initial_packet(Simple('Simple1').inputs['in'], 'foo')
