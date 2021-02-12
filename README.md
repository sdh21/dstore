
Dstore is a distributed storage service that aims to support fault-tolerant and durable key-value store and key-object store.

---

I have implemented

1.  A leader-based Multi-Paxos library optimized for

    - High-latency network:  
      The leader can concurrently do proposing, allowing services to work well under high-latency network.

    - Unstable servers:  
      The leader takeover is fast and immediate.  
      Any peer can become the leader at any time.  
      A peer can take over leadership and fill in all missing instances in one Paxos round.
      
    - Benchmarked 20,000 consensus per second.  
      See details here:
      [Benchmark](./docs/benchmark.md)

    - Correctness is the most important thing. I've simulated latency, missing/duplicate request/reply, network partition in tests, and I am still adding more tests, running fuzzing tests routinely and doing experiments on separate machines.

    - Compatible with Basic-Paxos. It can work without a leader correctly and will do its best to prevent collisions.

2.  A key-value database that supports

    - Mini-transaction:  
      An atomic unit that could include many operations

    - Complex operations:  
      operations including Get, Put, ListAppend, MapStore, MapDelete;  
      planning to support SortedMap, RangeQuery, If-else Statement, and arithmetic calculation.

    - DB-level batching:  
      A DB access layer provides transactions batching which wraps transactions from different clients into one proposal to alleviate Paxos sync overhead.

    - Benchmarked 200,000 mini-transactions per second with batch enabled.  
      See details here:
      [Benchmark](./docs/benchmark.md)



3.  A low-level custom storage layer that supports
    - Storing many small files without impacting performance; for example, storing Paxos logs, text files and so on.

    - Storing immutable large files; only retrieving/deleting is allowed.

---

I'm doing:

- [ ] Working on a demo that provides online file storage service

- [ ] Finishing Paxos & Key-Value DB crash recovery & quick restart

- [ ] Improving strategies regarding freeing Paxos in-memory instances in case one peer is down for a long time

- [ ] Working on membership change

- [ ] Optimizing for read-only transactions

---

I am planning to:

- [ ] Implement sharding

- [ ] Support transactions involving multiple tables/shards

