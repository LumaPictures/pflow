from abc import ABCMeta

class Packet(object):
    """
    Information packet (IP)
    """
    def __init__(self, value):
        """
        value:
        """
        self._value = value
        self._owner = None  # Component that owns this
        self.attrs = {}  # Named attributes

    @property
    def owner(self):
        return self._owner

    @owner.setter
    def owner(self, owner):
        if self._owner is not None:
            raise ValueError("You can not change a packet's owner. "
                             "Create a copy and drop this packet instead.")

        self._owner = owner

    @property
    def value(self):
        return self._value

    @value.setter
    def value(self, value):
        raise ValueError("You can not change a packet's value. "
                         "Create a copy and drop this packet instead.")

    def __repr__(self):
        return 'Packet({!r})'.format(self.value)


# FIXME: make this singleton
class EndOfStreamType(object):
    def __repr__(self):
        return 'END_OF_STREAM'

EndOfStream = EndOfStreamType()


# FIXME: should we enforce that BracketPackets have no value attribute?  Create a BasePacket with no .value?
# FIXME: create KeyedStartBracket and KeyedEndBracket for explicitness and easy type checking?
# FIXME: or should we add a type attribute to Packet? e.g. DATA, BEGIN_SUBSTREAM, END_SUBSTREAM, BEGIN_KEYED_SUBSTREAM, END_KEYED_SUBSTREAM
class BracketPacket(Packet):
    """
    Special packet used for bracketing.
    """
    __metaclass__ = ABCMeta

    def __init__(self, key=None):
        """
        key: an optional key used for random access bracketing (e.g. building dicts or arrays
                    without the need for strict stack behavior). Represents the begin/end of data
                    for a given key. Keyed and unkeyed brackets can be mixed,
                    but every keyed StartBracket must have a keyed EndBracket.
        """
        super(BracketPacket, self).__init__(self.__class__.__name__)
        self.key = key


class StartBracket(BracketPacket):
    """
    Start of bracketed data.
    """
    pass


class EndBracket(BracketPacket):
    """
    End of bracketed data.
    """
    pass
