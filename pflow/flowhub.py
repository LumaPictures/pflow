#!/usr/bin/env python
import os
import uuid
import logging
import socket

import requests

from . import utils

log = logging.getLogger(__name__)


class FlowhubClient(object):
    def __init__(self, endpoint='http://api.flowhub.io'):
        self._endpoint = endpoint
        self.log = logging.getLogger(self.__class__.__name__)

    def register_runtime(self, runtime_id, user_id, label, address):
        payload = {
            'id': runtime_id,
            'user': user_id,
            'label': label,
            'address': address,
            'protocol': 'websocket',
            'type': 'pflow',
            'secret': '9129923'  # unused
        }

        self.log.info('Registering runtime %s for user %s...' % (runtime_id, user_id))
        response = requests.put('%s/runtimes/%s' % (self._endpoint, runtime_id), payload)
        self._ensure_success(response)

    def ping_runtime(self, runtime_id):
        self.log.info('Pinging runtime %s...' % runtime_id)
        response = requests.post('%s/runtimes/%s' % (self._endpoint, runtime_id))
        self._ensure_success(response)

    @classmethod
    def create_runtime_id(cls):
        return uuid.uuid4().hex

    @classmethod
    def _ensure_success(cls, response):
        if not (199 < response.status_code < 300):
            raise Exception('Flow API returned error %d: %s' %
                            (response.status_code, response.text))


if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)

    ws_host = socket.gethostname()
    ws_port = utils.get_free_tcp_port()
    ws_address = 'ws://%s:%d' % (ws_host, ws_port)

    # TODO: create websocket server for fbp protocol handling
    log.info('Creating runtime websocket %s' % ws_address)

    label = 'pflow example'
    user_id = os.environ.get('FLOWHUB_USER_ID')
    runtime_id = os.environ.get('FLOWHUB_RUNTIME_ID')
    if not runtime_id:
        runtime_id = FlowhubClient.create_runtime_id()

    # Register runtime
    client = FlowhubClient()
    client.register_runtime(runtime_id, user_id, label, ws_address)
    client.ping_runtime(runtime_id)

    # TODO: keep websocket open and handle requests
