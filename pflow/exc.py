class FlowError(Exception):
    pass


class ComponentError(FlowError):
    pass


class ComponentInvalidError(ComponentError):
    pass
