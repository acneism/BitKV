# Configuration

CasketDB is configured with command-line flags. There is no configuration file and no `CONFIG SET`.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-addr` | `127.0.0.1:6379` | TCP address for clients. A non-loopback address without a password logs a warning |
| `-requirepass` | empty | Password for `AUTH`. If empty, `CASKETDB_REQUIREPASS` is used |
| `-dir` | `data` | Data directory |
| `-logs` | `0` (= 4) | Number of parallel logs for a new database. An existing database keeps the number stored in its `META`; a different non-zero value is an error |
| `-appendfsync` | `everysec` | `always`, `everysec` or `no`, see [persistence](persistence.md) |
| `-max-file-size` | 64 MB | Size at which a log rotates to a new data file |
| `-merge-ratio` | `0.5` | Share of dead bytes in a log that triggers an automatic merge |
| `-merge-min-bytes` | 64 MB | Minimum database size for an automatic merge, split evenly between logs |
| `-merge-interval` | `1m` | How often to check whether a log needs a merge; `0` turns automatic merge off |
| `-proto-max-bulk-len` | 512 MB | Largest bulk string a client may send |
| `-raft-id` | empty | Node id in the cluster. Setting it turns replication on |
| `-raft-peers` | empty | All cluster nodes including this one: `id=host:port,…`, the same on every node. Used only when the cluster is created and when a node joins; later the membership comes from the Raft log |
| `-raft-tls-cert` | empty | PEM certificate of this node for mutual TLS between nodes; its first DNS name must be the node id. Set together with `-raft-tls-key` and `-raft-tls-ca`, see [mutual TLS](replication.md#mutual-tls-between-nodes) |
| `-raft-tls-key` | empty | PEM private key of `-raft-tls-cert` |
| `-raft-tls-ca` | empty | PEM certificates of the CA that signs node certificates |
| `-raft-join` | `false` | Join a running cluster instead of creating one. `-raft-peers` lists this node and every current member; see [changing membership](replication.md#changing-membership) |
| `-raft-dir` | `<dir>/raft` | Raft log and snapshots |
| `-raft-listen` | the node's address in `-raft-peers` | Address the Raft transport listens on, when it differs from the address other nodes use, for example behind NAT or in a container |
| `-raft-election-timeout` | `1s` | Time without a leader before a node starts an election, at least 100 ms. Heartbeats go ten times as often. Lower it for faster failover on a fast network, raise it across slow links |
| `-raft-unsafe-no-fsync` | `false` | Do not fsync the Raft log. Faster, but see [replication](replication.md#running-without-fsync) |
| `-raft-reads` | `local` | Read consistency in a cluster: `local` (may be stale), `linearizable` or `lease`, see [consistent reads](replication.md#consistent-reads) |
| `-raft-max-clock-drift` | `0.1` | Largest relative difference between node clock rates that `-raft-reads lease` tolerates |

Sizes are given in bytes, for example `-max-file-size 134217728` for 128 MB. Durations use Go syntax: `30s`, `5m`.

## Environment

| Variable | Description |
| --- | --- |
| `CASKETDB_REQUIREPASS` | Password when `-requirepass` is not set. Keeps the password out of the process list |

## Examples

A single node that listens on all interfaces and fsyncs every write:

```bash
CASKETDB_REQUIREPASS=secret casketdb -addr 0.0.0.0:6379 -dir /var/lib/casketdb -appendfsync always
```

A local three-node cluster on one machine, one command per terminal:

```bash
casketdb -addr 127.0.0.1:6381 -dir n1 -raft-id n1 -raft-peers n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003
casketdb -addr 127.0.0.1:6382 -dir n2 -raft-id n2 -raft-peers n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003
casketdb -addr 127.0.0.1:6383 -dir n3 -raft-id n3 -raft-peers n1=127.0.0.1:7001,n2=127.0.0.1:7002,n3=127.0.0.1:7003
```

## Monitoring with INFO

`INFO` returns the sections below. Fields that clients commonly read, such as `redis_version` and `role`, keep their Redis names.

| Section | Fields |
| --- | --- |
| Server | `redis_version` (7.2.0, for client compatibility), `casketdb_version`, `redis_mode`, `os`, `process_id`, `uptime_in_seconds` |
| Clients | `connected_clients` |
| Persistence | `aof_enabled`, `appendfsync`, `bitcask_logs`, `bitcask_data_files`, `bitcask_total_bytes`, `bitcask_live_bytes`, `bitcask_merges`, `bitcask_writes`, `bitcask_fsyncs` |
| Stats | `total_connections_received`, `total_commands_processed`, `expired_keys` |
| Replication | `role` (`master` on the leader and on a single node, `slave` on followers), `raft_state`, `raft_term`, `raft_applied_index`, `raft_leader_id`, `raft_leader_addr`, `raft_membership` (`voter`, `learner` or `none`), `raft_voters`, `raft_learners` |
| Keyspace | `db0:keys=…,expires=…` |

`bitcask_total_bytes` minus `bitcask_live_bytes` is the space a merge can reclaim.

## Shutdown

`SIGINT` or `SIGTERM` stops the server cleanly: it closes the listener and client connections, stops Raft and fsyncs every log. A `kill -9` is also safe for data: see [persistence](persistence.md#crash-recovery).
