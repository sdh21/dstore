# Key-Value Store Component

Key-Value store with MVCC supported, as part of the store infrastructure.

It is built on Paxos, and 
can be used as a fault-tolerant configuration db.

However, it cannot by itself support sharding and distributed
transactions.

If you are looking for the implementation of a fully-functional kvdb,
see package shardkvdb.

I'm still working on sharding and 
distributed transactions.

