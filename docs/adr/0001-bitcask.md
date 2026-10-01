# 1. Bitcask as the storage engine

Status: accepted, 2026-09-25.

## Context

CasketDB needs durable writes, reads that cost at most one disk access, and recovery that a person can reason about. The candidates were an LSM tree (RocksDB, Pebble), a B-tree (bbolt) and Bitcask: an append-only log plus an in-memory index of every key.

## Decision

Bitcask, written in this repository. Writes append to a log; the key index maps every key to the file and offset of its latest record; merge rewrites the live records and drops the rest; hint files let the loader rebuild the index without reading values.

## Consequences

- A read is one lookup in memory and one read of a memory-mapped file. A write is a sequential append.
- Recovery is a scan of the logs, with torn tails cut off; there is no compaction state to repair.
- Every key lives in RAM, about 80–100 bytes plus the key. Data larger than memory needs another engine.
- Deleted and overwritten values take disk space until merge runs.
- The index is a hash map, so ordered data types (sorted sets, ranges) need an index of their own in memory.
