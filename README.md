# rafty-micro-cluster
![](https://img.shields.io/badge/Powered%20by-raft%20protocol-orange.svg?style=flat-square)  
A local, micro & distributed replica state machine(RSM) cluster **for learning** which implements *raft consensus algorithm* in a fully asynchronous way and does simple log replication.
### Introduction
#### Implementation
Using netty's event-driven udp for async communication among nodes inside the cluster.  

#### Related readings
- [CONSENSUS: BRIDGING THEORY AND PRACTICE  
D Ongaro - 2014 - stanford.edu](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
- [In search of an understandable consensus algorithm.  
D Ongaro, JK Ousterhout - USENIX Annual Technical Conference, 2014 - usenix.org](https://www.usenix.org/system/files/conference/atc14/atc14-paper-ongaro.pdf)
- [MIT PDOS 6.824 - Spring 2018](https://pdos.csail.mit.edu/6.824/)

### Progress
#### Finished
- Leader election/reelection (without restriction)
- Log replication

#### Ongoing
- Safety
- Client interaction

#### Todo

- Persisted states & recovery
- Cluster membership changes
- Leadership transfer extension
- Log compaction