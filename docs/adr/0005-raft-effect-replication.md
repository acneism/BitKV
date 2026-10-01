# 5. Raft, replicating the effects of commands

Status: accepted, 2026-09-26. The library changed from hashicorp/raft to github.com/acneism/raft on 2026-09-27.

## Context

Redis-style asynchronous replication loses acknowledged writes when the primary fails. CasketDB should survive the loss of a node without losing what it acknowledged.

## Decision

Replicate through Raft. Only `internal/replica` knows about Raft. The leader runs a command against its own data and proposes the effect, absolute operations such as "key k has value v until time t", not the command. It stages the values as proposed, visible only to writers of the same term, and answers once the entry is committed. Followers apply the operations in log order. Reads are local by default; linearizable and lease reads are options.

hashicorp/raft served from v0.5 to v0.9. From v0.10 CasketDB uses its own library, which compacts the log without snapshots ([ADR 6](0006-durable-index-marks.md)); since v0.11 it also provides membership changes, leadership transfer and read indexes.

## Consequences

- Followers apply without re-running command logic, clocks or randomness; INCR reaches them as the value it produced.
- An acknowledged write survives the loss of a minority of nodes.
- Only the leader accepts writes, and clients find it themselves.
- Each node expires keys by its own clock.
- Write throughput is bounded by the fsync of the Raft log, unless the operator accepts `-raft-unsafe-no-fsync`.
