#!/usr/bin/env python
import os
use_multi_process = (os.environ.get('PFLOW_MULTIPROCESS', '') == '1')

if use_multi_process:
    from pflow.executors.multi_process import MultiProcessGraphExecutor as GraphExecutorImpl
else:
    # Need to load before logging
    from pflow.executors.single_process import SingleProcessGraphExecutor as GraphExecutorImpl

import logging

import pflow
import pflow.utils
import example_graphs

log = logging.getLogger(__name__)


def run_graph(graph):
    # Write graphml file
    graphml_out_path = os.path.expanduser('~/graphml')
    if not os.path.exists(graphml_out_path):
        os.mkdir(graphml_out_path)
    graph.write_graphml('%s/%s.graphml' % (graphml_out_path, graph.name))

    log.info('Running graph: %s' % graph.name)
    log.debug('Runtime is: %s' % GraphExecutorImpl.__name__)

    # Execute graph
    executor = GraphExecutorImpl(graph)
    executor.execute()


def main():
    pflow.utils.init_logger(filename='example.log',
                            default_level=logging.DEBUG)

    sag = example_graphs.awesome.SuperAwesomeDemoGraph('AWESOME_1')

    test_graphs = [
        example_graphs.simple.SimpleGraph('SIMPLE'),
        sag,
        sag,
        # example_graphs.popular_music.PopularMusicGraph('MUSIC_1'),
        # example_graphs.mongo_poc.MongoPocGraph('MONGO_POC'),
        # example_graphs.process_spawning_logger.ProcessSpawningLogger('PROCSPAWN_1'),
    ]

    for graph in test_graphs:
        run_graph(graph)


if __name__ == '__main__':
    main()
