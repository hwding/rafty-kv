# rafty-micro-cluster
![](https://img.shields.io/badge/Powered%20by-raft%20protocol-orange.svg?style=flat-square)  
A local, micro & distributed replica state machine(RSM) cluster **for learning** which implements *raft consensus algorithm* in a fully asynchronous way and does simple log replication.
### Introduction
Using netty's event-driven udp for communication.

### Progress
#### Finished
- Basic leader election/reelection
#### Ongoing
- Log replication
#### Todo
- Client interaction
- Safety
- Persisted states & recovery
- Leadership transfer extension
- Cluster membership changes
- Log compaction
- Lock free
