# 3. A sharded key index and transactions that declare their keys

Status: accepted, 2026-09-26. Replaces the single lock of v0.1–v0.2.

## Context

Redis runs commands on one thread. CasketDB should use every core, without giving up the atomicity of a command or of MULTI/EXEC.

## Decision

Split the key index into 1024 shards, each with its own RWMutex and counters. Every command and every EXEC is a transaction that names its scope before it starts: the keys from the command's key specification, every shard, or one shard at a time for read-only scans. Shards are locked in ascending order. Touching a key outside the scope fails with `ErrNotLocked`.

## Consequences

- Commands on different keys run in parallel, and a transaction cannot deadlock.
- A command must know its keys before it runs. Scripts and other commands that find keys at run time will have to declare them, as Redis Cluster already requires.
- KEYS, SCAN and DBSIZE outside MULTI lock one shard at a time, so they are not a point-in-time view.
- Shards are also the unit of SCAN cursors, active expiry and the assignment of keys to logs ([ADR 4](0004-parallel-logs.md)).
