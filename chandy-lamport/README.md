# Chandy-Lamport Distributed Snapshots
An implementation of the [Chandy-Lamport Algorithm](https://lamport.azurewebsites.net/pubs/chandy.pdf) for distributed snapshots on top of a token passing system. The key idea of the algorithm is that servers send marker messages to each other to mark the beginning of the snapshot process and act as a stopper for recording messages.

## The algorithm
Starting the snapshot process on a server:
1. Record its local state
2. Send **marker messages** on all outbound interfaces

When recevie a **maker message**:
1. If haven't started the snapshot process yet, record the local state and send **maker messages** on all other interfaces
2. Start recording messages receiving on all other interfaces
3. Stop recording messages receving on this interface 

Terminate when all servers have received **maker messages** on all interfaces

## Assumptions
The algorithm makes the following assumptions:
* There are no failures and all messages arrive intact and only once
* The communication channels are unidirectional and ordered in the FIFO manner
* For any two servers in the system, there is a communication path between them
* The snapshot process does not interfere with the normal execution of the server
* Any server may initiate the global snapshot process

## Implementation
See `StartSnapshot()`, `HandlePacket()` in `server.go` and `StartShapshot()`, `NotifySnapshotComplete()`, `CollectSnapshot()` in `simulator.go`. The test code in `snapshot_test.go` reads the input file of events (`PassTokenEvent`s and `SnapshotEvent`s), injects the events to the simulator, and let the simlulator do the work making the events happen. The simulator progresses by `Tick()`ing. 

## Organization
The code is organized as follows:
* `server.go`: A process in the distributed system
* `simulator.go`: A discrete time simulator for the whole system
* `logger.go`: A logger that records events executed by the system
* `common.go`: Debug flag and ommon messages types used in the server, the logger, and the simulator
* `syncmap.go`: Implementatino of thread-safe map
* `snapshot_test.go`: Tests including ones for concurrent shapshotting
* `test_common.go`: Helper functions for testing
* `test_data/`: Test case inputs and expected results