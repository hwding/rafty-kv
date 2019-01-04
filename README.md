# rafty-micro-cluster
![](https://img.shields.io/badge/Powered%20by-Raft%20Algorithm-orange.svg?style=flat-square)  
A local running, distributed KV storage system for learning purposes based on replica state machine(RSM) cluster which implements raft consensus algorithm in a fully asynchronous way.
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
- Persisted states & recovery

#### Todo

- Cluster membership changes
- Leadership transfer extension
- Log compaction
