# 2. The Redis protocol and Redis semantics

Status: accepted, 2026-09-25.

## Context

A new database needs clients, command-line tools and benchmarks before anyone can use it. Its own protocol would need all of them written from scratch.

## Decision

Speak RESP2 and follow Redis 7: command names, arguments, replies, edge cases and error texts. Start with strings, keys, expiry and transactions; keep RESP3 and other data types for later. Where CasketDB adds something of its own, such as `RAFT`, it uses a command name Redis does not have.

## Consequences

- `redis-cli`, `redis-benchmark`, go-redis, redis-py and other clients work unchanged.
- Compatibility work never ends: every new command must match Redis, including the corner cases clients rely on, and differences are listed in [commands](../commands.md).
- Key positions come from a key specification per command, as in Redis, which is what makes declared-key transactions ([ADR 3](0003-sharded-keydir.md)) possible.
