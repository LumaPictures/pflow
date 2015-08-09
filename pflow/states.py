from enum import Enum

from . import exc


class ComponentState(Enum):

    # Comonent hasn't been initialized yet (initial state).
    NOT_INITIALIZED = 'NOT_INITIALIZED'

    # Component is initialized, but hasn't been run yet.
    INITIALIZED = 'INITIALIZED'

    # Component has received data and is actively running.
    ACTIVE = 'ACTIVE'

    # Component is waiting for data to send on its output port.
    SUSP_SEND = 'SUSP_SEND'

    # Component is waiting to receive data on its input port.
    SUSP_RECV = 'SUSP_RECV'

    # Component has successfully terminated execution (final state).
    TERMINATED = 'TERMINATED'

    # Component has terminated execution because of an error (final state).
    ERROR = 'ERROR'


def assert_component_state(*allowed_states):
    """
    Decorator that asserts the Component is in one of the given allowed_states
    before the method it wraps can be called.

    Parameters
    ----------
    allowed_states : ComponentState
    """
    allowed_states = set(allowed_states)

    def inner_fn(fn):
        @functools.wraps(fn)
        def wrapper(self, *args, **kwargs):
            try:
                state = self.state
            except AttributeError:
                try:
                    state = self.component.state
                except AttributeError:
                    raise RuntimeError("assert_component_state can only be "
                                       "used on component and port methods")
            if state not in allowed_states:
                raise exc.ComponentStateError(
                    self,
                    'method {} called in unexpected state {} '
                    '(expecting one of: {})'.format(
                        fn.__name__, self.state,
                        ', '.join(str(x) for x in allowed_states)))
            return fn(self, *args, **kwargs)

        return wrapper

    return inner_fn


def assert_not_component_state(*disallowed_states):
    """
    Decorator that asserts the Component is not in one of the given
    `disallowed_states` before the method it wraps can be called.

    Parameters
    ----------
    disallowed_states : ComponentState
    """
    allowed_states = set(ComponentState).difference(disallowed_states)
    return assert_component_state(*allowed_states)
