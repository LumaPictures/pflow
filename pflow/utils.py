import uuid
import socket


def get_free_tcp_port():
    '''
    Gets a free TCP port number on the system.

    Keep in mind that this is vulnerable to race conditions, but it's still useful
    when you need a free port assigned by the OS, and don't want to brute force a port range.
    '''
    sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sck.bind(('', 0))
    port = sck.getsockname()[1]
    sck.close()
    return port


def random_id():
    '''
    Generates a random hex string ID value.
    '''
    return uuid.uuid4().hex
