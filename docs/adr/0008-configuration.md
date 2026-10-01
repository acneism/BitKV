# 8. Flags and environment variables, node-local CONFIG SET

Status: accepted, 2026-10-01.

## Context

Operators configure CasketDB from the command line, containers and systemd units. Redis users also expect `CONFIG GET` and `CONFIG SET`. In a cluster, a setting changed on one node does not reach the others.

## Decision

Every flag can also come from the environment variable `CASKETDB_<FLAG>`; the command line wins, with a warning. There is no configuration file: `EnvironmentFile=` and `--env-file` cover that need. `CONFIG SET` changes a few settings that are safe to change at runtime (`requirepass`, `appendfsync`, `proto-max-bulk-len`) on the node it runs on, until restart, as in Redis. Paths, addresses and TLS files cannot be changed at runtime.

## Consequences

- Nodes of a cluster can drift apart: `CONFIG SET` must run on every node, and a restart forgets it.
- Settings shared by the whole cluster will be Raft entries stored with the data, built together with ACL users, which need the same replication.
- `CONFIG REWRITE` has nothing to write and is rejected.
