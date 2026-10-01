# Configuration

CasketDB is configured with command-line flags or the matching [environment variables](#environment). A few settings can be changed at runtime with `CONFIG SET`. There is no configuration file.

## Flags

| Flag | Default | Description |
| --- | --- | --- |
| `-addr` | `127.0.0.1:6379` | TCP address for clients without TLS; empty turns it off. A non-loopback address logs a warning: without a password anyone can connect, with one the password travels in plain text |
| `-tls-addr` | empty | TCP address for clients over TLS, see [TLS for clients](#tls-for-clients) |
| `-tls-cert` | empty | PEM certificate for `-tls-addr`, read again whenever its file changes |
| `-tls-key` | empty | PEM private key of `-tls-cert` |
| `-tls-ca` | empty | PEM certificates of the CA that signs client certificates. When set, every TLS client must present one |
| `-requirepass` | empty | Password for `AUTH`. Prefer `CASKETDB_REQUIREPASS`: flags are visible in the process list |
| `-dir` | `data` | Data directory |
| `-logs` | `0` (= 4) | Number of parallel logs for a new database. An existing database keeps the number stored in its `META`; a different non-zero value is an error |
| `-appendfsync` | `everysec` | `always`, `everysec` or `no`, see [persistence](persistence.md) |
| `-max-file-size` | 64 MB | Size at which a log rotates to a new data file |
| `-merge-ratio` | `0.5` | Share of dead bytes in a log that triggers an automatic merge |
| `-merge-min-bytes` | 64 MB | Minimum database size for an automatic merge, split evenly between logs |
| `-merge-interval` | `1m` | How often to check whether a log needs a merge; `0` turns automatic merge off |
| `-proto-max-bulk-len` | 512 MB | Largest bulk string a client may send |
| `-metrics-addr` | empty | Address of the Prometheus endpoint `/metrics`, for example `127.0.0.1:9121`; empty turns it off. See [monitoring](monitoring.md) |
| `-log-level` | `info` | `debug`, `info`, `warn` or `error` |
| `-log-format` | `text` | `text` (key=value) or `json`, one record per line on standard error |
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

Every flag can also come from an environment variable: `CASKETDB_` and the flag name in upper case, with dashes turned into underscores. `-raft-peers` is `CASKETDB_RAFT_PEERS`, `-appendfsync` is `CASKETDB_APPENDFSYNC`. A flag on the command line wins over the variable, and the server logs a warning naming the variable it ignored. A value the flag does not accept stops the server with the variable's name in the error.

`requirepass`, `appendfsync` and `proto-max-bulk-len` can also be changed at runtime with [`CONFIG SET`](commands.md#server-commands), on one node and until restart.

This is also the way to keep settings in a file: `EnvironmentFile=` in a systemd unit, `--env-file` in Docker. Pass the password as `CASKETDB_REQUIREPASS` rather than `-requirepass`, which the process list shows to every user.

## TLS for clients

`-tls-addr` opens a second listener that speaks TLS 1.2 or 1.3, like `tls-port` in Redis. Both listeners serve the same data, so a node can keep plain text on loopback for local tools and accept everyone else over TLS:

```bash
casketdb -addr 127.0.0.1:6379 -tls-addr 0.0.0.0:6380 -tls-cert server.crt -tls-key server.key
redis-cli -p 6380 --tls --cacert ca.crt
```

`-addr ""` leaves only TLS. With `-tls-ca`, a client must also present a certificate signed by that CA (`redis-cli --tls --cacert ca.crt --cert client.crt --key client.key`); this is in addition to the password, if one is set.

The server reads the certificate and key again when either file changes, at the next connection, so a renewed certificate needs no restart. If the new pair does not load, for example while only one of the two files has been replaced, the server keeps the previous certificate and logs a warning. The CA file is read only at start.

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
