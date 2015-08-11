
Subtreams
---------

**Legend**

symbol | meaning
-------|------------------
  `(`  | start substream
  `)`  | end substream
  `{`  | start map
  `}`  | end map
  `*`  | switch namespace (destination name implied, but not shown)
  `.`  | data acquired by row

Consider the following stream of data:

```
1 2 a 3 b c 4 5 d
```

We can add hierarchical structure to a stream of data by injecting
start `(` and end `)` control brackets into the stream.

For example:

```
1 2 ( a ) 3 ( b c ) 4 5 ( d )
```

The resulting structure is equivalent to this python list:

```python
['1', '2', ['a'], '3', ['b', 'c'], '4', '5', ['d']]
```

The chart below separates data packets and bracketing packets into
separate rows to reinforce a key concept of bracketing packets: they do not in any
way alter the data packets themselves, they surround and lend stucture to them.

```
--------------------------------------------------
| <data>      |   1 2   a   3   b c   4 5   d    |
|-------------|----------------------------------|
| <brackets>  |   . . ( . ) . ( . . ) . . ( . )  |
--------------------------------------------------
```


Stream Maps
-----------

A map is a container for substreams that allows the generation of multiple
sibling substreams simultaneously.  It is particularly useful for streaming
structured data out of order, such as data acquired from an asynchronous process.

A map has one active namespace at a time (akin to a key in a `dict`, or more
accuractely a `defaultdict(list)`), into which all incoming packets
are placed.  The active namespace is set via a special "switch" control packet
(marked by `*` in the charts below).  

The power of a map is that it allows a
component to add items to a substream, switch to a different substream at the same
level, add more items there, then return to the orignal and continue building
where it left off.  In effect, it manages a cursor that can be moved between
mutliple substreams at the same level.  Something akin to a map could be achieved
using substreams by repeatedly opening and closing brackets, but a map ensures
that each substream has a single start and end bracket, which is essential for
many component operations.

A map has its
own bracketing packets (denoted by `{` and `}`) and must be explictily started
and ended. If a "switch namespace" packet arrives when no open map
exists it is an error. Once a map has been started and a namespace set, normal
substreams (`(` and `)`) are created within it, and can be nested.

Because maps have a definitive beginning and end, they can themselves be nested within
substreams.  Namespaces are strictly local to the currently open map, and cannot
refer to higher-level maps (just as the keys within a dictionary cannot point
into a different dictionary)

Here is an example which separates the stream of input characters into alpha and
numeric substreams. 

```
-------------------------------------------------------------------------
| <data>     | namespace |         1 2     a   3   b c   4 5     d      |
|------------|-----------|----------------------------------------------|
|            | num:      |       ( . .         .         . . )          |
| <brackets> | _control_ |   { *       *     *   *     *       *     }  |
|            | alpha:    |               ( .       . .           . )    |
-------------------------------------------------------------------------
```

The resulting structure resembles this python dictionary:

```python
{
    'num': ['1', '2', '3', '4', '5'],
    'alpha': ['a', 'b', 'c', 'd']
}
```

Channels
========

Simply put, a channel is the sequence of control packets within a stream.
As we know, control packets can be intermixed with data packets to add structure
to the stream without the alteration or cooperation of data
packets, which are immutable.  Because of their separate, descriptive nature, it 
is possible to strip out a "channel" of control packets from the stream and completely replace
it, thereby providing a completely different structure to the surrounding data.  

The power of channels in pflow is that multiple mutually exclusive channels can
exist within the same stream of packets, allowing components to choose which representation
of the data that they subscribe to.  Unlike the namespaces of a map, the entire
data stream is represented in each channel.  This allows for problems
to be solved without branching, which has the side-effect of parallel execution
and is not always desired, and avoids merging, which can be prone to error.

A component subscribes to a particular representation of the data via a named
"channel".  Control packets are assigned a channel when they are created (by 
default, this is the "default" channel).  When a component receives
control packets which do not belong to the component's subscribed input channel
they are automatically skipped and passed downtream untouched.  While it can
be useful to think of a channel as a composite of data packets and bracket packets
it is more accurate to say that a channel is a filtering out of control packets in
foreign channels.

The chart below merges the examples from above into a stream of packets that
combine hierarchical and mapped stuctures, placing each into their own channel.

```
-----------------------------------------------------------------------------------------------
|            | channel  | namespace |                                                         |
|============|==========|===========|=========================================================|
| <data>     |          |           |        1 2       a     3     b c     4 5       d        |
|------------|----------|-----------|---------------------------------------------------------|
|            | default  |           |        . . (     . )   . (   . . )   . . (     . )      |
|            |----------|-----------|---------------------------------------------------------|
| <brackets> |          | num:      |      ( . .             .             . .   )            |
|            | alphanum | <control> |  { *         *       *     *       *         *       }  |
|            |          | alpha:    |                ( .           . .               .   )    |
-----------------------------------------------------------------------------------------------
```

