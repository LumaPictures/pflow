
Streams
=======

Consider the following stream of data:

```
1 2 a 3 b c 4 5 d
```

There are a handful of ways that we might want to structure this data.  

Hierarchical Streams (aka Sub-Streams)
--------------------------------------

**Legend**

symbol | meaning
-------|------------------
  `(`  | start sub-stream
  `)`  | end sub-stream
  `{`  | start map-stream
  `}`  | end map-stream
  `.`  | data acquired by channel
  `*`  | switch map key (destination key implied, but not shown)

We can add hierarchical structure to a stream of data by interjecting
start `(` and end `)` "bracketing packets" into the stream:

```
1 2 ( a ) 3 ( b c ) 4 5 ( d )
```

The resulting structure resembles this python list:

```python
['1', '2', ['a'], '3', ['b', 'c'], ['4', '5'], ['d']]
```

For the sake of the discussion below, we can separate data and bracketing
packets into separate rows:

```
--------------------------------------------------
| <data>      |   1 2   a   3   b c   4 5   d    |
|-------------|----------------------------------|
| <brackets>  |   . . ( . ) . ( . . ) . . ( . )  |
--------------------------------------------------
```

This demonstrates a key concept of bracketing packets: they do not in any
way alter the data packets themselves: they surround and lend stucture to them.

Map Streams
--------------

A map stream directs packets into separate namespaces. While a
hierarchical stream requires packets to be sent in
depth-first order, a mapped stream can thus accomodate input streams with
breadth-first sorting, as well as completely arbitary sorting.

A map stream has one active namespace at a time, into which all incoming packets
are placed. The active namespace is set via a special "switch" contol packet
(marked by `*` in the charts below).  A map stream has its
own bracketing packets (denoted by `{` and `}`), and must be explictily started
and ended. Using a switch packet to change namespaces when no open map
stream exists is an error. Once a map stream has been started, normal hierachical sub-streams
(`(` and `)`) must be created within it.

Here is an example which separates the stream of input characters into alpha and
numeric sub-streams. 

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

Bracket Channels
================

In the examples above, it was explained that control packets rest on top of
the stream of data packets, and they do not require their alteration or cooperation.
Because of their separate, descriptive nature, it is possible for multiple
control streams to coexist, thereby offering differing representations of a data
stream's structure.

A component subscribes to a particular representation of the data via a named
"channel".  Control packets are assigned a channel when they are sent
downstream by a component.  When receiving control packets, packets which do not
belong to the component's subscribed channel are automatically skipped and
passed downtream. All components inherit a default channel from the base
`Component` class.

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

