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
    def __init__(self, component, msg):
        self.component = component
        self.msg = msg

    def __str__(self):
        return '{}: {}'.format(self.component, self.msg)


class ComponentStateError(ComponentError):
    """
    Component-level state/transition error.
    """
    pass


class PortError(ComponentError):
    """
    Port error.
    """
    def __init__(self, port, msg):
        self.port = port
        self.msg = msg

    def __str__(self):
        return '{}: {}'.format(self.port, self.msg)


class PortClosedError(PortError):
    """
    Port is closed.
    """
    pass


class PortTimeout(PortError):
    """
    Port communication timed out.
    """
    pass
