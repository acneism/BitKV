# 4. Parallel logs with two-phase commit across them

Status: accepted, 2026-09-26.

## Context

With one log, every write waits for one stream of writes and fsyncs. On Windows an fsync also blocks concurrent writes to the same file.

## Decision

Write to N independent logs, 4 by default, each with its own active file, group commit, fsync and merge. Shard i belongs to log i mod N, and N is fixed when the database is created. A batch that touches several logs uses two-phase commit: a header in every part, then a commit record in every involved log once all parts are written. Recovery keeps a batch only if some log holds its commit record.

## Consequences

- Pure writes are about 40–45% faster with 4 logs than with one.
- A transaction across logs costs two write rounds and about 60 bytes per log, so related keys are cheaper in one log.
- With `appendfsync everysec` or `no`, a power loss can keep one part of a cross-log transaction and lose another.
- Changing the number of logs needs an offline rewrite of the data.
