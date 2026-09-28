# Changelog

Versions are listed newest first. CasketDB was called BitKV up to and including v0.9.

## Unreleased

### Added

- Consistent reads: `-raft-reads linearizable` confirms every read with the leader, so it sees every write acknowledged before it started, on any node. `-raft-reads lease` lets the leader answer from its lease without a network round, assuming clock rates differ by at most `-raft-max-clock-drift`. A read that no leader confirms returns `TRYAGAIN`.
- `RAFT TRANSFER [id]` hands leadership over to another voter before the leader is stopped for maintenance.
- Membership changes at runtime: start a node with `-raft-join`, then `RAFT ADDLEARNER`, `RAFT PROMOTE` and `RAFT REMOVE` on the leader. `RAFT MEMBERS` lists the members; `INFO replication` shows `raft_membership`, `raft_voters` and `raft_learners`.
- Mutual TLS between nodes with `-raft-tls-cert`, `-raft-tls-key` and `-raft-tls-ca`. A node must present a certificate for its own id; the server checks its certificate at start and warns when Raft runs without TLS on a non-loopback address.
- `-raft-listen` sets the Raft listen address when it differs from the node's address in `-raft-peers`; `-raft-election-timeout` sets the election timeout, 1 s by default.
- Fault-injection tests: real server processes under `kill -9`, network partitions, leadership transfers and membership changes, with the RESP client history checked for linearizability by Porcupine. See [CONTRIBUTING](CONTRIBUTING.md#fault-injection-tests).

### Changed

- Replication runs on github.com/acneism/raft v0.3.1. Nodes now negotiate the wire protocol version, so later upgrades can roll through the cluster one node at a time.
- `raft_leader_addr` in `INFO` comes from the cluster configuration stored in the Raft log instead of `-raft-peers`.

### Fixed

- A write command that changed nothing, such as DEL of a missing key or SETNX of an existing one, could answer on the strength of another client's write that was proposed but not yet committed. Had that write been lost in a leader change, the answer would have been wrong; a consistent read right after it could also contradict it. Such commands now wait until the proposed writes they read are committed. Found by the new fault-injection tests.

### Upgrading from v0.10

- The wire format between nodes changed: a cluster cannot mix v0.10 nodes with newer ones. Stop all nodes, upgrade them, and start them again. Data and Raft directories open unchanged.

## v0.10 — 2026-09-28

### Changed

- The project is renamed from BitKV to CasketDB. The Go module is `github.com/acneism/casketdb`, the binary `casketdb`, the password variable `CASKETDB_REQUIREPASS`, and the INFO field `casketdb_version`.
- Replication runs on the Raft library [github.com/acneism/raft](https://github.com/acneism/raft) v0.2.1. hashicorp/raft and raft-wal are gone; CasketDB has no other external dependency.
- Snapshots are taken only when a follower falls behind the start of the leader's log, instead of every 2^20 entries.
- New data directories get the `META` signature `casketdb-meta 1`. Directories with `bitkv-meta 1` still open.

### Added

- Durable index: every log stores Raft index marks, so a node knows exactly which Raft entries are on disk in all logs. The mark rides on the existing fsync; no fsync is added to the write path.
- The Raft log is compacted without snapshots, 65,536 entries behind the durable index. A restart replays only the entries after the durable index instead of up to about a million.
- Project documentation: README, [docs/](docs/README.md), CONTRIBUTING, SECURITY and this changelog, and a logo.

### Security

- Directories are created with mode `0700` and files with `0600`. They used to be `0755` and `0644`, so on Linux every local user could read the data. The server logs a warning at start if an existing `-dir` or Raft directory is open to other users; run `chmod 700` on it.

### Upgrading from v0.9

- **Single node:** the data directory opens as is.
- **Cluster:** the Raft directory of v0.9 is rejected at start (`ErrOldRaftLog`), and a node whose data directory has keys but no Raft state refuses to start (`ErrNotEmpty`). Start a new cluster from empty directories and load the data through a client.

## v0.9 — 2026-09-26

- Speculative writes on the leader: a write releases its key locks as soon as it is proposed, and the next write to the same key builds on the proposed value. INCR of one hot key in a 3-node cluster went from 0.72k to 18.8k ops/s on Windows with fsync.
- Snapshots hard-link data files instead of copying them: 200k keys take about 29 ms instead of 217–260 ms, and 70 KB of memory instead of 59 MB.
- Fixed: a node that crashed while restoring a snapshot kept a partial database and diverged from the cluster. The restore is now marked and repeated on the next start.
- Fewer allocations per command: SET 10 → 6, GET 12 → 7, INCR 9 → 6.

## v0.8 — 2026-09-26

- Reads come from memory: sealed data files are memory-mapped, and the active file is mirrored in memory. A single-threaded GET went from 7 µs to 1 µs on Windows.
- Followers apply Raft entries in batches, up to 2.7× faster.

## v0.7 — 2026-09-26

- `-raft-unsafe-no-fsync` skips the fsync of the Raft log for speed, at the cost of safety on power loss.

## v0.6 — 2026-09-26

- Consecutive pipelined writes of one connection run as one transaction: one group commit, and one Raft entry in a cluster.
- The Raft log moved from raft-boltdb to raft-wal: one fsync per batch instead of at least two. Raft directories of v0.5 are rejected at start.

## v0.5 — 2026-09-26

- Replication through Raft (hashicorp/raft) for clusters of 3 or 5 nodes: a write is acknowledged after a majority has it in the Raft log.
- Followers answer `READONLY`; `INFO replication` shows the Raft state and the leader.
- Flags `-raft-id`, `-raft-peers` and `-raft-dir`.

## v0.4 — 2026-09-26

- Data is written to several independent logs, 4 by default (`-logs`), each with its own group commit, fsync and merge.
- Batches that span logs use two-phase commit and stay atomic after a crash.
- `FLUSHDB` is atomic through a marker file.
- Single-log directories from earlier versions still open.

## v0.3 — 2026-09-26

- No global data lock: the key index is split into 1024 shards with their own locks, and transactions declare the keys they touch.
- Active expiry walks the shards with a cursor, as Dragonfly does. Time comes from an injectable clock.

## v0.2 — 2026-09-25

- Group commit with an in-memory overlay: writers no longer queue behind each other's fsync on Windows.
- Transactions: MULTI, EXEC, DISCARD, WATCH, UNWATCH.
- Password protection: `AUTH` and `-requirepass`.
- The server listens on `127.0.0.1` by default.

## v0.1 — 2026-09-25

- First version: Bitcask storage (append-only data files, an in-memory key index, merge with hint files, CRC on every record), a RESP2 server, string, key and expiry commands, `appendfsync` policies and crash recovery.
