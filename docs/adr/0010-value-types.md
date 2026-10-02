# 10. Value types in records, collections as one value first

Status: accepted, 2026-10-02.

## Context

Hashes, sets, lists, sorted sets and streams need a type on every key, so that commands of one type refuse keys of another with `WRONGTYPE` and `TYPE` can answer. Data written by earlier versions holds only strings and must open unchanged, and nodes of a cluster must not misread each other's entries during an upgrade.

A collection can be stored in Bitcask in three ways: serialized whole as the value of its key; as one record per field, next to a record for the key; or in a second engine, such as an LSM tree. One record per field makes every change small, but each field becomes an entry of the in-memory key index, a key needs a way to list its fields, and deleting a key means deleting every field. A whole value keeps one entry per key and reuses transactions, expiry, merge, snapshots and replication as they are, but every change rewrites the whole collection.

## Decision

The type is part of the record. A record with flag 64 carries the type in the first byte of its value, with the numbers Redis uses: 1 list, 2 set, 3 sorted set, 4 hash, 6 stream. A string has no flag and no prefix, so every existing record reads as a string. The key index does not store the type: commands that need it read the record, as they read the value anyway. `META` moves to `casketdb-meta 2` when a directory is opened, so an older version refuses a directory that may hold typed records. Raft entries with typed operations get a new kind, which an older node refuses instead of storing the value as a string.

A collection is stored as one value, like a listpack in Redis. Large collections will later move to one record per field, as Redis moves from a listpack to a hash table, behind the same commands.

## Consequences

- Strings cost nothing: no extra byte, no change to the read path or the index.
- `TYPE`, `OBJECT ENCODING` and `SCAN … TYPE` read the record of every key they look at.
- Until large collections get their own encoding, a change to a collection rewrites it whole, in the data file and in the Raft log, so a collection of many thousands of elements is slow to change.
- Once a newer version has opened a directory, v0.13 and older refuse it. In a cluster, every node must run the new version before the first command that writes a typed value.
