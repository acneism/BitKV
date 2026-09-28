# Limitations and roadmap

CasketDB is early software. This page lists what it does not do yet, and how each limitation could be lifted. Nothing here is scheduled; it is a map, not a promise.

## Out of scope for v1

These are deliberate choices, not missing features:

- sharding data across nodes, as Redis Cluster does;
- data types other than strings: lists, hashes, sets, sorted sets, streams;
- Lua scripting and pub/sub;
- RESP3;
- databases other than `db 0`.

## Known limitations

| Limitation | Possible fix |
| --- | --- |
| All keys must fit in RAM: about 80–100 bytes plus the key length per key | Inherent to Bitcask; a disk-based index would be a different engine |
| One user and one password; no TLS for clients | ACL users and TLS through `crypto/tls` |
| A cross-log transaction costs two write rounds plus about 60 bytes of header and commit per log | Hash tags like `{user}:…` in Redis Cluster, so that related keys land in one log |
| With `everysec` or `no`, a power loss can break the atomicity of a cross-log transaction | Fsync the parts before the commit records |
| The number of logs is fixed when a database is created | Offline redistribution of keys to a new number of logs |
| KEYS, SCAN and DBSIZE outside MULTI are not a point-in-time snapshot | MVCC versions, or KEYS under an all-shard lock |
| Merge checks every record's liveness with a separate shard lock | Batches grouped by shard |
| WATCH fires spuriously after a merge and misses a key created and deleted in between | Per-shard version counters |
| FLUSHDB, CONFIG, INFO, SELECT and similar commands are rejected inside MULTI | Run them after the transaction commits |
| Expiry depends on the system clock | Detect a backward clock jump at start |
| No online backup | A backup command built on the hard-link snapshots |
| Snapshots hold hard links, so disk space of files deleted by merge is freed only when the snapshot is dropped | Keep one snapshot, or align merges with snapshots |
| Clients find the leader themselves, from `INFO replication` or a `READONLY` reply | Proxy writes to the leader, or reply with its address |
| Each node expires keys by its own clock | NTP; if needed, expiry as Raft entries from the leader |
| An existing single-node database cannot join a cluster | Import through a snapshot when the cluster starts |
| With `-appendfsync no`, a node may refuse to start after a power loss, because the Raft log was compacted past the surviving data | Start the node from an empty directory; or fsync data before compacting the Raft log |
| Restoring a snapshot on a follower goes through a temporary database: about twice the key index in memory and a full rewrite of the data | Swap data files and rebuild the index in place |
| No production track record, no end-to-end fault-injection test of the cluster with network partitions and clock skew | A Jepsen-style test with Porcupine, as the Raft library already has |
