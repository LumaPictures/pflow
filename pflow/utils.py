import uuid
import socket


def get_free_tcp_port():
    sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sck.bind(('', 0))
    port = sck.getsockname()[1]
    sck.close()
    return port


def random_id():
    return uuid.uuid4().hex
