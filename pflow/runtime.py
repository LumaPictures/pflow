#!/usr/bin/env python
from .executors.single_process import SingleProcessGraphExecutor

import os
import uuid
import logging
import time
import json
from abc import ABCMeta, abstractmethod
from collections import OrderedDict
import inspect

import requests
import gevent
import geventwebsocket

from . import exc, core, utils

log = logging.getLogger(__name__)


class Runtime(object):
    """
    Manages components, graphs, and executors.
    """
    def __init__(self):
        self.log = logging.getLogger(self.__class__.__name__)

        self._components = {}  # Component metadata, keyed by component name
        self._graphs = {}  # Graph instances, keyed by graph ID
        self._executors = {}  # GraphExecutor instances, keyed by graph ID

        self.log.debug('Initialized runtime!')

    @property
    def all_component_specs(self):
        specs = {}
        for component_name, component_options in self._components.iteritems():
            specs[component_name] = component_options['spec']

        return specs

    def register_component(self, component_class, overwrite=False):
        """

        :param component_class:
        :param collection:
        :param overwrite:
        """
        if not issubclass(component_class, core.Component):
            raise ValueError('component_class must be a class that inherits from Component')

        collection = component_class.__module__
        normalized_name = '{0}/{1}'.format(component_class.__module__,
                                           component_class.__name__)

        if normalized_name in self._components and not overwrite:
            raise exc.RuntimeError("Component {0} already registered".format(normalized_name))

        self.log.debug('Registering component: {0}'.format(normalized_name))

        self._components[component_class.__name__] = {
            'class': component_class,
            'spec': self._create_component_spec(component_class)
        }

    def register_module(self, module, overwrite=False):
        """

        :param module:
        :param collection:
        :param overwrite:
        """
        if isinstance(module, basestring):
            module = __import__(module)

        if not inspect.ismodule(module):
            raise ValueError('module must be either a module or the name of a module')

        self.log.debug('Registering module: %s' % module.__name__)

        registered = 0
        for obj_name in dir(module):
            class_obj = getattr(module, obj_name)
            if (inspect.isclass(class_obj) and
                    (class_obj != core.Component) and
                    (not inspect.isabstract(class_obj)) and
                    (not issubclass(class_obj, core.Graph)) and
                    issubclass(class_obj, core.Component)):
                self.register_component(class_obj, overwrite)
                registered += 1

        if registered == 0:
            self.log.warn('No components were found in module: %s' % module.__name__)

    @classmethod
    def _create_component_spec(cls, component_class):
        from . import core
        if not issubclass(component_class, core.Component):
            raise ValueError('component_class must be a Component')

        component = component_class('FAKE_NAME')

        spec = {
            'description': (component.__doc__ or ''),
            'inPorts': [
                {
                    'id': inport.name,
                    'type': 'any',  # TODO
                    'description': (inport.description or ''),
                    'addressable': isinstance(inport, core.ArrayInputPort),
                    'required': (not inport.optional)
                }
                for inport in component.inputs
            ],
            'outPorts': [
                {
                    'id': outport.name,
                    'type': 'any',  # TODO
                    'description': (outport.description or ''),
                    'addressable': isinstance(outport, core.ArrayOutputPort),
                    'required': (not outport.optional)
                }
                for outport in component.outputs
            ]
        }

        return spec

    def is_started(self, graph_id):
        if graph_id not in self._executors:
            return False

        return self._executors[graph_id].is_running()

    def start(self, graph_id):
        """
        Execute a graph.
        """
        self.log.debug('Graph %s: Starting execution' % graph_id)

        graph = self._graphs[graph_id]

        if graph_id not in self._executors:
            # Create executor
            self.log.info('Creating executor for graph %s...' % graph_id)
            executor = self._executors[graph_id] = SingleProcessGraphExecutor(graph)
        else:
            executor = self._executors[graph_id]

        if executor.is_running():
            raise ValueError('Graph %s is already started' % graph_id)

        executor.execute()

    def stop(self, graph_id):
        """
        Stop executing a graph.
        """
        self.log.debug('Graph %s: Stopping execution' % graph_id)
        if graph_id not in self._executors:
            raise ValueError('Invalid graph: %s' % graph_id)

        executor = self._executors[graph_id]
        executor.stop()

    def _create_or_get_graph(self, graph_id):
        if graph_id not in self._graphs:
            self._graphs[graph_id] = core.Graph(graph_id)

        return self._graphs[graph_id]

    def _find_component_by_name(self, graph, component_name):
        for component in graph.components:
            if component.name == component_name:
                return component

    def new_graph(self, graph_id):
        """
        Create a new graph.
        """
        self.log.debug('Graph %s: Initializing' % graph_id)
        self._graphs[graph_id] = core.Graph(graph_id)

    def add_node(self, graph_id, node_id, component_id):
        """
        Add a component instance.
        """
        # Normally you'd instantiate the component here,
        # we just store the name
        self.log.debug('Graph %s: Adding node %s(%s)' % (graph_id, component_id, node_id))

        graph = self._create_or_get_graph(graph_id)

        component_class = self._components[component_id]['class']
        component = component_class(node_id)
        graph.add_component(component)

    def remove_node(self, graph_id, node_id):
        """
        Destroy component instance.
        """
        self.log.debug('Graph %s: Removing node %s' % (graph_id, node_id))

        graph = self._create_or_get_graph(graph_id)
        graph.remove_component(node_id)

    def add_edge(self, graph_id, src, tgt):
        """
        Connect ports between components.
        """
        self.log.debug('Graph %s: Connecting ports: %s -> %s' % (graph_id, src, tgt))

        graph = self._graphs[graph_id]

        source_component = self._find_component_by_name(graph, src['node'])
        source_port = source_component.outputs[src['port']]

        target_component = self._find_component_by_name(graph, tgt['node'])
        target_port = target_component.inputs[tgt['port']]

        graph.connect(source_port, target_port)

    def remove_edge(self, graph_id, src, tgt):
        """
        Disconnect ports between components.
        """
        self.log.debug('Graph %s: Disconnecting ports: %s -> %s' % (graph_id, src, tgt))

        graph = self._graphs[graph_id]

        source_component = self._find_component_by_name(graph, src['node'])
        source_port = source_component.outputs[src['port']]
        if source_port.is_connected():
            graph.disconnect(source_port)

        target_component = self._find_component_by_name(graph, tgt['node'])
        target_port = target_component.inputs[tgt['port']]
        if target_port.is_connected():
            graph.disconnect(target_port)

    def add_iip(self, graph_id, src, data):
        """
        Set the inital packet for a component inport.
        """
        self.log.debug('Graph %s: Setting IIP to "%s" on port %s' % (graph_id, data, src))

        graph = self._graphs[graph_id]

        target_component = self._find_component_by_name(graph, src['node'])
        target_port = target_component.inputs[src['port']]
        if target_port.is_connected():
            graph.disconnect(target_port)

        graph.set_initial_packet(target_port, data)

    def remove_iip(self, graph_id, src):
        """
        Remove the initial packet for a component inport.
        """
        self.log.debug('Graph %s: Removing IIP from port %s' % (graph_id, src))

        graph = self._graphs[graph_id]

        target_component = self._find_component_by_name(graph, src['node'])
        target_port = target_component.inputs[src['port']]
        if target_port.is_connected():
            graph.disconnect(target_port)

        graph.unset_initial_packet(target_port)


class RuntimeApplication(geventwebsocket.WebSocketApplication):
    """
    Web socket application that hosts a single Runtime.
    """
    def __init__(self, ws):
        super(RuntimeApplication, self).__init__(self)

        self.log = logging.getLogger(self.__class__.__name__)

        # TODO: Use a factory for creating this object, so that a Registry can be instantiated outside the WebSocketApplication constructor.
        self.runtime = Runtime()

        from . import components
        self.runtime.register_module(components)

    ### WebSocket transport handling ###
    @staticmethod
    def protocol_name():
        # WebSocket sub-protocol
        return 'noflo'

    def on_open(self):
        self.log.info("Connection opened")

    def on_close(self, reason):
        self.log.info("Connection closed. Reason: %s" % reason)

    def on_message(self, message, **kwargs):
        if not message:
            self.log.warn('Got empty message')

        m = json.loads(message)
        dispatch = {
            'runtime': self.handle_runtime,
            'component': self.handle_component,
            'graph': self.handle_graph,
            'network': self.handle_network
        }

        try:
            handler = dispatch[m.get('protocol')]
        except KeyError:
            self.log.warn("Subprotocol '%s' not supported" % p)
        else:
            handler(m['command'], m['payload'])

    def send(self, protocol, command, payload):
        """
        Send a message to UI/client
        """
        self.ws.send(json.dumps({'protocol': protocol,
                                 'command': command,
                                 'payload': payload}))

    ### Protocol send/responses ###
    def handle_runtime(self, command, payload):
        # Absolute minimum: be able to tell UI info about runtime and supported capabilities
        if command == 'getruntime':
            payload = {
                'type': 'fbp-python-example',
                'version': '0.4',  # protocol version
                'capabilities': [
                    'protocol:component',
                    'protocol:network'
                ],
            }
            self.send('runtime', 'runtime', payload)

        # network:packet, allows sending data in/out to networks in this runtime
        # can be used to represent the runtime as a FBP component in bigger system "remote subgraph"
        elif command == 'packet':
            # We don't actually run anything, just echo input back and pretend it came from "out"
            payload['port'] = 'out'
            self.send('runtime', 'packet', payload)

        else:
            self.log.warn("Unknown command '%s' for protocol '%s' " % (command, 'runtime'))

    def handle_component(self, command, payload):
        # Practical minimum: be able to tell UI which components are available
        # This allows them to be listed, added, removed and connected together in the UI
        if command == 'list':
            for component_name, component_data in self.runtime.all_component_specs.iteritems():
                payload = component_data
                payload['name'] = component_name
                self.send('component', 'component', payload)

        else:
            self.log.warn("Unknown command '%s' for protocol '%s' " % (command, 'component'))

    def handle_graph(self, command, payload):
        # Modify our graph representation to match that of the UI/client
        # Note: if it is possible for the graph state to be changed by other things than the client
        # you must send a message on the same format, informing the client about the change
        # Normally done using signals,observer-pattern or similar

        send_ack = True

        # New graph
        if command == 'clear':
            self.runtime.new_graph(payload['id'])
        # Nodes
        elif command == 'addnode':
            self.runtime.add_node(payload['graph'], payload['id'], payload['component'])
        elif command == 'removenode':
            self.runtime.remove_node(payload['graph'], payload['id'])
        # Edges/connections
        elif command == 'addedge':
            self.runtime.add_edge(payload['graph'], payload['src'], payload['tgt'])
        elif command == 'removeedge':
            self.runtime.remove_edge(payload['graph'], payload['src'], payload['tgt'])
        # IIP / literals
        elif command == 'addinitial':
            self.runtime.add_iip(payload['graph'], payload['tgt'], payload['src']['data'])
        elif command == 'removeinitial':
            self.runtime.remove_iip(payload['graph'], payload['tgt'])
        # Exported ports
        elif command in ('addinport', 'addoutport'):
            pass  # No support in this example
        # Metadata changes
        elif command in ('changenode',):
            pass
        else:
            send_ack = False
            self.log.warn("Unknown command '%s' for protocol '%s' " % (command, 'graph'))

        # For any message we respected, send same in return as acknowledgement
        if send_ack:
            self.send('graph', command, payload)

    def handle_network(self, command, payload):
        def send_status(cmd, g):
            started = self.runtime.is_started(g)
            # NOTE: running indicates network is actively running, data being processed
            # for this example, we consider ourselves running as long as we have been started
            running = started
            payload = {
                graph: g,
                started: started,
                running: running,
            }
            self.send('network', cmd, payload)

        graph = payload.get('graph', None)
        if command == 'getstatus':
            send_status('status', graph)
        elif command == 'start':
            self.runtime.start(graph)
            send_status('started', graph)
        elif command == 'stop':
            self.runtime.stop(graph)
            send_status('started', graph)
        else:
            self.log.warn("Unknown command '%s' for protocol '%s'" % (command, 'network'))


class RuntimeRegistry(object):
    """
    Represents a Runtime registry.
    """
    @abstractmethod
    def register_runtime(self, runtime_id, user_id, label, address):
        pass

    def ping_runtime(self, runtime_id):
        pass

    @classmethod
    def create_runtime_id(cls):
        return uuid.uuid4().hex

    @classmethod
    def _ensure_http_success(cls, response):
        if not (199 < response.status_code < 300):
            raise Exception('Flow API returned error %d: %s' %
                            (response.status_code, response.text))


class FlowhubRegistry(RuntimeRegistry):
    """
    FlowHub runtime registry.
    It's necessary to use this if you want to manage your graph in either FlowHub or NoFlo-UI.
    """
    def __init__(self):
        self._endpoint = 'http://api.flowhub.io'
        self.log = logging.getLogger(self.__class__.__name__)

    def register_runtime(self, runtime_id, user_id, label, address):
        payload = {
            'id': runtime_id,

            'label': label,
            'type': 'pflow',

            'address': address,
            'protocol': 'websocket',

            'user': user_id,
            'secret': '9129923',  # unused
        }

        self.log.info('Registering runtime %s for user %s...' % (runtime_id, user_id))
        response = requests.put('%s/runtimes/%s' % (self._endpoint, runtime_id),
                                data=json.dumps(payload),
                                headers={'Content-type': 'application/json'})
        self._ensure_http_success(response)

    def ping_runtime(self, runtime_id):
        self.log.info('Pinging runtime %s...' % runtime_id)
        response = requests.post('%s/runtimes/%s' % (self._endpoint, runtime_id))
        self._ensure_http_success(response)


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    logging.getLogger('requests').setLevel(logging.WARN)

    #ws_host = socket.gethostname()
    #ws_port = utils.get_free_tcp_port()
    ws_host = 'localhost'
    ws_port = 3569
    ws_address = 'ws://%s:%d' % (ws_host, ws_port)
    client = FlowhubRegistry()

    def runtime_websocket_server():
        """
        This greenlet runs the websocket server that responds to runtime commands.
        """
        r = geventwebsocket.Resource(OrderedDict([('/', RuntimeApplication)]))
        s = geventwebsocket.WebSocketServer(('', ws_port), r)
        log.info('Runtime listening on %s' % ws_address)
        s.serve_forever()

    def registry_pinger():
        """
        This greenlet occasionally pings the registered runtime to keep it alive.
        """
        while True:
            client.ping_runtime(runtime_id)
            gevent.sleep(30)  # Ping every 30 seconds

    label = 'foo bar'
    user_id = os.environ.get('FLOWHUB_USER_ID')
    runtime_id = os.environ.get('FLOWHUB_RUNTIME_ID')
    if not runtime_id:
        runtime_id = FlowhubRegistry.create_runtime_id()

    # Register runtime
    client.register_runtime(runtime_id, user_id, label, ws_address)

    greenlets = [
        gevent.spawn(runtime_websocket_server),
        gevent.spawn(registry_pinger)
    ]

    gevent.wait(greenlets)
