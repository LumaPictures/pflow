class FlowError(Exception):
    """
    Error in the flow design.
    """
    pass


class GraphExecutorError(FlowError):
    pass


class ComponentError(Exception):
    """
    Component-level error.
    """
    def __init__(self, component, message):
        super(ComponentError, self).__init__(message)
        self.component = component

    def __str__(self):
        return '{}: {}'.format(self.component, self.message)


class ComponentStateError(ComponentError):
    """
    Component-level state/transition error.
    """
    pass


class PortError(ComponentError):
    """
    Port error.
    """
    def __init__(self, port, message):
        super(PortError, self).__init__(port.component, message)
        self.port = port

    def __str__(self):
        return '{}: {}'.format(self.port, self.message)


class PortClosedError(PortError):
    """
    Port is closed.
    """
    def __init__(self, port):
        super(PortClosedError, self).__init__(port, 'Port is closed')


class PortTimeout(PortError):
    """
    Port communication timed out.
    """
    def __init__(self, port):
        super(PortTimeout, self).__init__(port, 'Port communication timed out')
