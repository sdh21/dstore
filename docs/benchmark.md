

# Benchmark

- [Benchmark](#benchmark)
    - [Setting 1 - EC2 c9d.xlarge](#setting-1---ec2-c9dxlarge)
        - [Part 1. Paxos](#part-1-paxos)
        - [Part 2. Key-Value DB](#part-2-key-value-db)
    - [Setting 2 - Low latency, 2 cores 4 threads](#setting-2---low-latency-2-cores-4-threads)
        - [Part 1. Paxos](#part-1-paxos-1)
        - [Part 2. Key-Value DB](#part-2-key-value-db-1)
    - [Setting 3 - High latency network](#setting-3---high-latency-network)



## Setting 1 - EC2 c9d.xlarge

Configuration:

- Three servers are running on three c5d.9xlarge EC2 instances.

  (36 vCPUs, 72 GB memory, nvme SSD, 10 Gigabit network, 26000 fsync/s)

- Clients are running on one c5.9xlarge instance.

  (36 vCPUs, 72 GB memory, 10 Gigabit network)

- All EC2 instances are in the same availability zone.

- TLS enabled

- Each test runs for at least 2 minutes

### Part 1. Paxos

- Only the leader is proposing
- 16 concurrent proposals at the same time

  | Proposal size |  Consensus per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 1 KB |   20224   |   21396  | 
  | 4 KB |   17429   |   17512  |
  | 16 KB |  9501   |   9623  |

<br>

### Part 2. Key-Value DB

- Each transaction has 4 MapStore operations and 3 ListAppend operations
- Value with different size (1KB, 4KB, 16KB) is put in the last ListAppend operation
- Every client must wait for server response before sending a new transaction
- Each client is writing its own table

* **1 KB Value**

  | Clients |  Transaction per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 1 |   1255   |   1271  | 
  | 4 |   5418   |   5477  |
  | 16 |  14130   |   14295  |
  | 64 |  18537   |   18840  |

* **4 KB Value**

  | Clients |  Transaction per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 1 |   1108   |   1138  | 
  | 4 |   4962   |   5007  |
  | 16 |  11582   |   11669  |
  | 64 |  11141   |   11588  |

* **16 bytes Value (batch enabled, 16 queues)**

  | Clients |  Transaction per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 1024 |    153967  |  157244  |
  | 4096 |    196163  |  204414  |
  | 16384 |   229715  |  241046  |

* **1 KB Value (batch enabled, 16 queues)**

  | Clients |  Transaction per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 64 |  29373   |   29514  |
  | 256 | 57923   |   59253  |
  | 1024 | 75330  |   76922  |
  | 4096 | 101046 |   103288 |
  | 16384 | 108876 |  122239 |

<br>

## Setting 2 - Low latency, 2 cores 4 threads

Configuration:

- 3 Paxos servers running on 3 Vultr cloud servers

- Each server has 4 vCore, 16GB memory, nvme SSD

- Servers are in the same region, with about 0.4ms latency

- Clients are running on one additional Vultr server in the same region

### Part 1. Paxos

- Only the leader is proposing
- 16 concurrent proposals at the same time
- Each test runs for at least 10 minutes

  | Proposal size |  Consensus per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 1 KB |   8143   |   8557  | 
  | 4 KB |   7669   |   7813  |
  | 16 KB |  5140   |   5212  |

<br>

### Part 2. Key-Value DB

- Each transaction has 4 MapStore operations and 3 ListAppend operations
- Value with different size (1KB, 4KB, 16KB) is put in the last ListAppend operation
- Every client must wait for server response before sending a new transaction
- Each client is writing its own table
- Each test runs for at least 2 minutes; TPS is sampled every 10 seconds

* **1 KB Value**

  | Clients |  Transaction per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 1 |   892   |   913  | 
  | 4 |   3140   |   3292  |
  | 16 |  4538   |   4926  |
  | 64 |  5347   |   5519  |

* **4 KB Value**
  
  | Clients |  Transaction per second (minimum)  | (maximum) |
  | ----------- |  :----------: | :------- |
  | 1 |   854   |   876  |
  | 4 |   3097   |   3171  |
  | 16 |  4396   |   4550  |
  | 64 |  4772   |   5077  |

* **16 KB Value**
  
  | Clients |  Transaction per second (minimum)  | (maximum) |
  | ----------- |  :----------: | :------- |
  | 1 |   775   |   788  |
  | 4 |   2416   |   2515  |
  | 16 |  3176   |   3352  |
  | 64 |  3561   |   3801  |

* **1 KB Value (batch enabled, 4 concurrent queues)**

  | Clients |  Transaction per second (minimum)  | (maximum) |
  | ----------- |  :----------: | :------- |
  | 64 |  21195   |   22209  |
  | 256 | 39313   |   41130  |
  | 1024 | 52243  |   56021  |

<br>

## Setting 3 - High latency network

Configuration:

- 3 Paxos servers running on 3 Vultr servers

- Each server has 4 vCore, 16GB memory, nvme SSD

- Servers are in different regions, with about 200ms latency and limited bandwidth. The designated leader is in Chicago and the other two acceptors are in Sydney.

- Client is in the same region with the server designated as leader.

- To cope with high-latency network, the number of concurrent clients is increased.

Key-Value DB Performace:

* **1 KB Value**

  | Clients |  Transaction per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 64 |   297   |   307  | 
  | 512 |   2190   |   2375  |
  | 1024 |  3021   |   3297  |

* **1 KB Value (batch enabled, 256 concurrent queues)**

  | Clients |  Transaction per second (minimum)  | (maximum) |
    | ----------- |  :----------: | :------- |
  | 2048 |  4221   |   4413  |
  | 8192 |  4357   |   4736  |

  ( Possibly limited by bandwidth. )

<br>
