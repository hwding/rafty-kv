# rafty-kv
![](https://img.shields.io/badge/Powered%20by-Raft%20Algorithm-orange.svg?style=flat-square)  
A local running, distributed KV store for learning purposes based on replica state machine(RSM) cluster which implements raft consensus algorithm in a fully asynchronous way.
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
- Persisted states & recovery

#### Ongoing
- Safety
- Client interaction
- Log compaction

#### Todo
- Cluster membership changes
- Leadership transfer extension

### How-To
#### Node start up argument
```text
[current node port] [other node ports ...]

e.g.
(node 0) 8080 8081 8082 8083 8084
(node 1) 8081 8080 8082 8083 8084
(node 2) 8082 8080 8081 8083 8084
(node 3) 8083 8080 8081 8082 8084
(node 4) 8084 8080 8081 8082 8083
```
#### Cluster/Node global configuration
Configuration will be auto loaded from resource path.

Specifying conf file in startup argument is currently not supported, for that using different configurations among nodes is not suggested.

Default conf: [rafty-node.properties](https://github.com/hwding/rafty-kv/blob/master/src/main/resources/rafty-node.properties)
