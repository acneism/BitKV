# CasketDB documentation

Start with the [project README](../README.md) for a quick start.

## Using CasketDB

- [Commands and differences from Redis](commands.md) — what is supported and where behavior differs
- [Configuration](configuration.md) — flags, environment, INFO fields
- [Persistence and recovery](persistence.md) — fsync policies, crash recovery, merge, backups
- [Replication](replication.md) — running a Raft cluster, guarantees, failover
- [Monitoring](monitoring.md) — Prometheus metrics, useful queries, logs
- [Limitations and roadmap](limitations.md) — what CasketDB does not do yet

## Inside CasketDB

- [Architecture](architecture.md) — packages, transactions, logs, reads, expiry, merge, replication internals, on-disk format
- [Benchmarks](benchmarks.md) — measured numbers and how to compare builds

## Project

- [Contributing](../CONTRIBUTING.md)
- [Security policy](../SECURITY.md)
- [Changelog](../CHANGELOG.md)
