# 11. Members of large collections as records, tied by a generation

Status: accepted, 2026-10-02. Adds the second encoding promised by [ADR 10](0010-value-types.md).

## Context

A collection stored as one value is rewritten whole on every change. Large collections need each member, a hash field or a set member, in a record of its own, so that a change writes one small record and one small Raft entry. The members must not appear as keys: SCAN, DBSIZE and the key index must not see them, and a user key with the same bytes must not clash with them. Deleting or expiring a collection must not write a tombstone per member, and the loader must still drop the members of a collection that no longer exists, also when a cross-log transaction is rolled back at start.

## Decision

A member is a record with flag 128 whose key is the collection key, a generation and the member, and whose value is the member's value. The record of the collection key itself has a kind with the high bit set, and its value starts with the 8-byte generation. A collection gets a new random generation each time it is created, so the members of an earlier collection under the same key carry another generation.

In memory, each key with members has a table of them next to the key index, in the same shard, with its own overlay and proposed values. Deleting, expiring or overwriting the key drops its table, and so does a new generation. The loader collects members without regard to the key, applies the rollbacks of uncommitted transactions to them like to keys, and at the end keeps only the members whose generation matches the key's current record. Merge copies a member when the table still points at it. A hint file marks a member with the high bit of the key length. A Raft entry carries member operations in the typed kind, after the operation on their key.

## Consequences

- Deleting a large collection writes one tombstone; merge reclaims its members later.
- Members cost about as much memory as keys: the member, an index entry and map overhead, with the value on disk.
- A change to a member also rewrites the small record of its key, which keeps the count and lets WATCH notice the change.
- The start of a database reads the record of every key that has members, to learn its generation.
