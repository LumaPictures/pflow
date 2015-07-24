class FlowError(Exception):
    '''
    Error in the flow design.
    '''
    pass


class ComponentError(Exception):
    '''
    Component-level error.
    '''
    pass


class PortError(FlowError):
    '''
    Port error.
    '''
    pass


class PortClosedError(PortError):
    '''
    Port is closed.
    '''
    pass
