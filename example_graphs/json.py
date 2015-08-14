from pflow.components import *
import textwrap

class JsonGraph(Graph):

    def initialize(self):
        fromjson = FromJSON('from')
        tojson = ToJSON('to')
        self.connect(fromjson.outputs['OUT'], tojson.inputs['IN'])
        self.set_initial_packet(
            fromjson.inputs['IN'],
            textwrap.dedent("""\
            {
                "num": [1, 2, 3, 4, 5],
                "alpha": ["a", "b", "c", "d"],
                "zero": 0
            }""")
        )
