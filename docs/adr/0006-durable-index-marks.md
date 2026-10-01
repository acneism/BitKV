# 6. Compacting the Raft log behind the engine's durable index

Status: accepted, 2026-09-27.

## Context

The Raft log cannot grow forever. The usual answer, periodic snapshots of the whole state machine, copies data the engine has already written to its own logs.

## Decision

The engine writes index marks into its logs when it syncs, and reports the lowest mark that every log has made durable. The Raft library compacts its log up to that index, keeping a margin of 65,536 entries, without taking a snapshot. A snapshot is taken only when a follower falls behind the start of the log, and it is made of hard links to the data files plus a list of their lengths, so it costs time per file, not per byte.

## Consequences

- A restarted node replays only the tail of the log after its durable index.
- Merge and FLUSHDB must carry the latest mark into the new active file before dropping old files.
- With `appendfsync no`, the durable index follows writes instead of fsyncs, so after a power loss the Raft log may already be compacted past the data that survived.
- A snapshot keeps files alive through its hard links, so space freed by merge returns only when the snapshot is dropped.
- The library reuses its latest snapshot for every lagging node; a learner added after that snapshot cannot use it ([limitations](../limitations.md)).
