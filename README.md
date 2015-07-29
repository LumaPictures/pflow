# pflow

Python [flow-based programming](http://www.jpaulmorrison.com/fbp/) implementation that tries to remain as close
to the "classic" approach as possible.

[![Build Status](https://travis-ci.org/Flushot/pflow.svg)](https://travis-ci.org/Flushot/pflow)

**THIS PROJECT IS STILL IN ITS VERY EARLY STAGES**

To quote J. Paul Morrison:
> In computer programming, Flow-Based Programming (FBP) is a programming paradigm that uses a "data factory" metaphor 
for designing and building applications. FBP defines applications as networks of "black box" processes, which exchange 
data across predefined connections by message passing, where the connections are specified externally to the processes. 
These black box processes can be reconnected endlessly to form different applications without having to be changed 
internally. FBP is thus naturally component-oriented.

![Flow-based programming example](./docs/fbp-example.jpg)

## How is this useful?

Drag-and-drop programming!

You can define data flow execution graphs where each process (node) is run in parallel. To define these graphs, you can
use a GUI like [DrawFBP](https://github.com/jpaulm/drawfbp), [NoFlo UI](https://github.com/noflo/noflo-ui), 
or [Flowhub](https://flowhub.io/).

## Getting Started

Run `python setup.py develop` to symlink site-packages to this repo, 
then run the example graphs with `./example.py`.

## Components

TODO: Improve documentation.

To define a component, subclass `pflow.core.Component` then override the `initialize()` and `run()` methods:

    from pflow.core import Component, InputPort, OutputPort
    
    
    class MySleepComponent(Component):
        '''
        Receives input from IN, sleeps for a predetermined amount of time,
        then forwards it to output OUT.
        '''
        def initialize(self):
            self.inputs.add(InputPort('IN'))
            self.outputs.add(OutputPort('OUT'))
           
        def run(self):
            input_packet = self.inputs['IN'].receive_packet()
            time.sleep(5)
            self.outputs['OUT'].send_packet(input_packet)

### Component Design

Here's some general rules of thumb for creating components:

* The `Component.initialize()` method is used for setting up ports and any initial state.
* The `Component.run()` method is called by the runtime every time there's a new packet arrives on the `InputPort`
  and hasn't been received yet.
* Components will automatically suspend execution after `Component.run()` completes, and will await the next packet
  unless there is no more data to receive and all upstream components have been terminated. In that case, the component
  will terminate execution.
* Call `Component.suspend()` if you need to be explicit about suspending execution (typically done in loops or when 
  waiting for some asynchronous task to complete).
* Calls to `InputPort.receive()` or `InputPort.receive_packet()` implicitly call `Component.suspend()` while waiting
  for data to arrive. Calls to `OutputPort.send()` or `OutputPort.send_packet()` do not have this behavior, however.
* Call `Component.terminate()` if you need to be explicit about terminating a component.
* Unless you are explicitly calling `Component.suspend()` to wait on an async result, and are making a call to a 
  blocking operation (that gevent patches), you should add a `self.state = ComponentState.SUSPENDED` before the call.

### Component States

![Component states](./docs/states.png)

| State | Description |
| ----- | ----------- |
| **NOT_STARTED** | Component hasn't received any data yet (initial state) |
| **ACTIVE** | Component has received data and is actively running |
| **SUSPENDED** | Component execution has been suspended while it waits for data |
| **TERMINATED** | Component has terminated execution (final state) |
| **ERROR** | Component has terminated execution with an error (final state) |
