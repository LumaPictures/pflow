import uuid
import socket


def get_free_tcp_port():
    """
    Gets a free TCP port number on the system.

    Keep in mind that this is vulnerable to race conditions, but it's still useful
    when you need a free port assigned by the OS, and don't want to brute force a port range.
    """
    sck = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sck.bind(('', 0))
    port = sck.getsockname()[1]
    sck.close()
    return port


def random_id():
    """
    Generates a random hex string ID value.
    """
    return str(uuid.uuid4())


def pluck(seq, key):
    """
    Extracts a list of property values from a sequence.

    :param seq: the sequence or iterator to pluck the value from.
    :param key: the dict key or object attribute name to use for plucking.
    :return: list of extracted values.
    """
    vals = []

    for item in seq:
        if isinstance(item, dict):
            vals.append(item[key])
        else:
            vals.append(getattr(item, key))

    return vals


def init_logger(default_level=None, filename=None, logger_levels=None):
    import logging

    console_format = '%(processName)-20s | %(levelname)-5s | %(name)s: %(message)s'
    file_format = '%(asctime)s | %(processName)-20s | %(levelname)-5s | %(name)s: %(message)s'

    # File logger
    if default_level is None:
        default_level = logging.DEBUG

    logging_args = {
        'level': default_level
    }
    if filename is not None:
        logging_args.update({
            'filename': filename,
            'filemode': 'w',
            'format': file_format
        })
    else:
        logging_args.update({
            'format': console_format
        })

    logging.basicConfig(**logging_args)

    if filename is not None:
        # Console logger
        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        console.setFormatter(logging.Formatter(console_format))
        logging.getLogger('').addHandler(console)

    # Set verbosity levels
    if logger_levels is None:
        logger_levels = {
            # Reduce verbosity for chatty packages
            'requests': logging.WARN,
            'geventwebsocket': logging.INFO,
            'sh': logging.WARN

            # 'pflow.core': logging.INFO,
            # 'pflow.components': logging.INFO,
            # 'pflow.executors': logging.INFO
        }

    for logger_name, logger_level in logger_levels.items():
        logging.getLogger(logger_name).setLevel(logger_level)
