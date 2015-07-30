class FlowError(Exception):
    """
    Error in the flow design.
    """
    pass


class ComponentError(Exception):
    """
    Component-level error.
    """
    pass


class ComponentStateError(ComponentError):
    """
    Component-level state/transition error.
    """
    pass


class PortError(FlowError):
    """
    Port error.
    """
    pass


class PortClosedError(PortError):
    """
    Port is closed.
    """
    pass


class GraphRuntimeError(PortError):
    pass
