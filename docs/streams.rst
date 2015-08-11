
Subtreams
---------

**Legend**

+------------------+------------------------------------------------------------+
| symbol           | meaning                                                    |
+==================+============================================================+
| ``(`` / ``)``    | start / end substream                                      |
+------------------+------------------------------------------------------------+
| ``{`` / ``}``    | start / end map                                            |
+------------------+------------------------------------------------------------+
| ``*``            | switch namespace (destination name implied, but not shown) |
+------------------+------------------------------------------------------------+
| ``"``            | data acquired by row                                       |
+------------------+------------------------------------------------------------+

Consider the following stream of data::

    1 2 a 3 b c 4 5 d

We can add hierarchical structure to this stream of data by injecting
start ``(`` and end ``)`` control brackets into the stream. For example::

    1 2 ( a ) 3 ( b c ) 4 5 ( d )

The resulting structure is equivalent to this python list:

.. code:: python

    ['1', '2', ['a'], '3', ['b', 'c'], '4', '5', ['d']]


The chart below separates data packets and bracketing packets into
separate rows to reinforce a key concept of bracketing packets: they do not in any
way alter the data packets themselves, which are immutable, they surround and
lend stucture to them.

+-------------+-----------------------------------------------------------------------------------------+
| Packet Type | Stream                                                                                  |
+=============+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+
| data        |``1``|``2``|     |``a``|     |``3``|     |``b``|``c``|     |``4``|``5``|     |``d``|     |
+-------------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
| brackets    |     |     |``(``|     |``)``|     |``(``|     |     |``)``|     |     |``(``|     |``)``|
+-------------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+


Stream Maps
-----------

A map is a container for substreams that enables the generation of multiple
sibling substreams simultaneously.  It is particularly useful for streaming
structured data out of order, such as data acquired from an asynchronous process.

A map has one active namespace at a time (akin to a key in a ``dict``, or more
accuractely a ``defaultdict(list)``), into which all incoming packets
are placed.  The active namespace is set via a special "switch" control packet
(marked by ``*`` in the charts below).  

The power of a map is that it allows a component to add items to a substream,
switch to a different substream, add more items there, then return to the orignal and continue building
where it left off.  In effect, it manages a cursor that can be moved between
mutliple substreams at the same level.  Something akin to a map could be achieved
using only substreams by repeatedly opening and closing brackets, but a map ensures
that each substream has a single start and end bracket, which is essential for
many component operations.

A map has its own bracketing packets distinct from substreams (denoted by ``{``
and ``}``) and must be explictily started
and ended. If a "switch namespace" packet arrives when no open map
exists it is an error. Once a map has been started and a namespace set, normal
substreams (``(`` and ``)``) are created within it, and can be nested.

Because maps have a definitive beginning and end, they can themselves be nested within
substreams.  Namespaces are strictly local to the currently open map, and cannot
refer to other maps (just as the keys within a dictionary cannot point
into a different dictionary)

Here is an example which separates the stream of input characters into alpha and
numeric substreams. 

+-------------+-----------+-----------------------------------------------------------------------------------------------------------------------------+
| Packet Type | Namespace | Stream                                                                                                                      |
+=============+===========+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+
| data        |           |     |     |     |``1``|``2``|     |     |``a``|     |``3``|     |``b``|``c``|     |``4``|``5``|     |     |``d``|     |     |
+-------------+-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
| brackets    | num       |     |     |``(``|``"``|``"``|     |     |     |     |``"``|     |     |     |     |``"``|``"``|``)``|     |     |     |     |
+             +-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
|             | _control_ |``{``|``*``|     |     |     |``*``|     |     |``*``|     |``*``|     |     |``*``|     |     |     |``*``|     |     |``}``|
+             +-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
|             | alpha     |     |     |     |     |     |     |``(``|``"``|     |     |     |``"``|``"``|     |     |     |     |     |``"``|``)``|     |
+-------------+-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+



The resulting structure resembles this python dictionary:

.. code:: python

    {
        'num': ['1', '2', '3', '4', '5'],
        'alpha': ['a', 'b', 'c', 'd']
    }


Channels
========

As discussed earlier, control packets can be inserted between data packets to add structure
to a stream. Because they are a secondary superimposition of structure onto a
constant data stream, these streams of control packets are, when taken as a whole,
interchangeable.  In other words, it is possible to strip out all of the control packets
from a stream and completely replace them, thereby providing an alterate
structure to the same source data.

For example, we could remove the following substreams which separate letters from numbers::

    1 2 ( a ) 3 ( b c ) 4 5 ( d )


And instead replace them with substreams that group adjacent letter-number pairs::


    1 ( 2 a ) ( 3 b ) ( c 4 ) ( 5 d )


We can take the idea of alternate represenations one step further. Instead of
replacing the control packets or creating branching
streams (which sometimes has the undesired side-effect of parallel execution)
we can allow multiple mutually exclusive sequences of control packets to coexist
within the same stream of packets.  The combination of any one of these control
streams with the data stream is a channel.  Components can
then choose which representation of the data that they subscribe to. 

Unlike the namespaces of a map, the entire
data stream is represented in each channel.  Also, unlike maps, the names of
channels are somewhat fixed, and are often a property of the component.

It can be useful to think of a channel as a composite of data packets and
bracket packets, but it is important to keep in mind that both bracket and data
packets are sent along the same wire, so to speak.  It is therefore more accurate
to think of a channel as the filtering out of control packets from
foreign channels.  It is also important to note that components should propagate
control packets from foreign channels downstream, since the entire point of
having multiple representations is that they survive long enough to reach a
component that requires them.

.. ..

    This allows for problems
    to be solved without branching, 
    and is not always desired, and avoids merging, which can be prone to error.
    Control packets are assigned a channel when they are created (by 
    default, this is the "default" channel).  When a component receives
    control packets which do not belong to its subscribed input channel
    they are skipped and passed downtream untouched.  

The chart below merges the substream and map examples from above into a single
stream of packets, placing each representation into its own channel.

**Be sure to scroll to the right to see the whole chart**

+-------------+-----------+-----------+-----------------------------------------------------------------------------------------------------------------------------------------------------------------+
| Packet Type | Channel   | Namespace | Stream                                                                                                                                                          |
+=============+===========+===========+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+=====+
| data        |           |           |     |     |     |``1``|``2``|     |     |     |``a``|     |     |``3``|     |     |``b``|``c``|     |     |``4``|``5``|     |     |     |``d``|     |     |     |
+-------------+-----------+-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
|             | default   |           |     |     |     |``"``|``"``|``(``|     |     |``"``|``)``|     |``"``|``(``|     |``"``|``"``|``)``|     |``"``|``"``|``(``|     |     |``"``|``)``|     |     |
+             +-----------+-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
| brackets    |           | num       |     |     |``(``|``"``|``"``|     |     |     |     |     |     |``"``|     |     |     |     |     |     |``"``|``"``|     |``)``|     |     |     |     |     |
+             +           +-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
|             | alphanum  | _control_ |``{``|``*``|     |     |     |     |``*``|     |     |     |``*``|     |     |``*``|     |     |     |``*``|     |     |     |     |``*``|     |     |     |``}``|
+             +           +-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+
|             |           | alpha     |     |     |     |     |     |     |     |``(``|``"``|     |     |     |     |     |``"``|``"``|     |     |     |     |     |     |     |``"``|     |``)``|     |
+-------------+-----------+-----------+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+-----+

