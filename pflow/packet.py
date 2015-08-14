from abc import ABCMeta

DEFALT_PACKET_CHANNEL = 'default'


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
class ControlPacket(Packet):
    """
    Special packet used for bracketing.
    """
    __metaclass__ = ABCMeta

    def __init__(self, channel=DEFALT_PACKET_CHANNEL):
        """
        channel : str
        """
        super(ControlPacket, self).__init__(self.__class__.__name__)
        self.channel = channel


class StartSubStream(ControlPacket):
    """
    Start of bracketed data.
    """
    pass


class EndSubStream(ControlPacket):
    """
    End of bracketed data.
    """
    pass


class StartMap(ControlPacket):
    """
    Start of bracketed data.
    """
    pass


class EndMap(ControlPacket):
    """
    End of bracketed data.
    """
    pass


class SwitchMapNamespace(ControlPacket):
    def __init__(self, namespace, channel=DEFALT_PACKET_CHANNEL):
        """
        namespace : str

            an name used for random access bracketing (e.g. building dicts or arrays
            without the need for strict stack behavior). Represents the begin/end of data
            for a given key. Keyed and unkeyed brackets can be mixed,
            but every keyed StartSubStream must have a keyed EndSubStream.


        Keyed brackets can be used to create something like a dictionary (or
        `defaultdict(list)` to be more exact).
        """
        super(SwitchMapNamespace, self).__init__(channel)
        self.namespace = namespace
