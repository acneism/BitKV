# CasketDB

CasketDB is a Redis-compatible key-value store written in Go. It keeps values on disk in a Bitcask log, runs commands on all cores, and can replicate through Raft so that a failed node never takes acknowledged writes with it.

Any Redis client — `redis-cli`, `redis-benchmark`, go-redis, redis-py — works with CasketDB unchanged, within the [supported commands](docs/commands.md).

> **Status: early.** CasketDB v0.10 is covered by unit, model, fuzz and cluster fault tests, but it has not run in production yet. It supports string commands only. Read [limitations](docs/limitations.md) before relying on it.
>
> The project was called BitKV until 2026-09-28.

## Contents

- [Why CasketDB](#why-casketdb)
- [Quick start](#quick-start)
- [Running a cluster](#running-a-cluster)
- [When to use it](#when-to-use-it)
- [Documentation](#documentation)
- [Performance](#performance)
- [Contributing](#contributing)
- [License](#license)

## Why CasketDB

- **Data larger than RAM.** Values live on disk; memory holds only the key index (about 80–100 bytes plus the key length per key). Restart reads keys from hint files, not the values.
- **All cores.** 1024 lock shards instead of a single command thread, and four parallel logs with group commit.
- **No lost acknowledged writes in a cluster.** With 3 or 5 nodes, a write is acknowledged only after a majority has it in the Raft log. Redis replication is asynchronous; CasketDB's is not.
- **Familiar durability.** `appendfsync always | everysec | no` with Redis semantics. An acknowledged write survives `kill -9` in every mode and a power loss according to the fsync policy.
- **Small and dependency-light.** One binary. The only external dependency is the Raft library [github.com/acneism/raft](https://github.com/acneism/raft).

## Quick start

Build from source (Go 1.26.1 or newer):

```bash
git clone https://github.com/acneism/casketdb
cd casketdb
go build -o casketdb ./cmd/casketdb
./casketdb -dir data
```

Connect with any Redis client:

```bash
redis-cli SET greeting hello
redis-cli GET greeting
```

The server listens on `127.0.0.1:6379` by default; change it with `-addr`. On a non-loopback address, set a password with `-requirepass` or the `CASKETDB_REQUIREPASS` environment variable; without one CasketDB logs a warning. All flags are listed in [configuration](docs/configuration.md).

## Running a cluster

Start three nodes with the same `-raft-peers` and empty directories. The cluster forms by itself:

```bash
casketdb -addr 10.0.0.1:6379 -dir data -raft-id n1 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
casketdb -addr 10.0.0.2:6379 -dir data -raft-id n2 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
casketdb -addr 10.0.0.3:6379 -dir data -raft-id n3 -raft-peers n1=10.0.0.1:7000,n2=10.0.0.2:7000,n3=10.0.0.3:7000
```

Only the leader accepts writes; followers answer `-READONLY`. `INFO replication` shows the leader's id and address. Every node serves reads, which may lag behind the leader. Details are in [replication](docs/replication.md).

## When to use it

CasketDB fits when:

- the data is larger than RAM, but the keys fit;
- losing an acknowledged write after a failover is unacceptable — idempotency keys, balances, counters, sessions, rate limits;
- you want one multi-core node without setting up Redis Cluster.

Redis, Valkey or Dragonfly fit better when you need hashes, lists, sets, sorted sets, pub/sub or scripting, the lowest possible latency on in-memory data, sharding across many nodes, ACLs and TLS.

## Documentation

User guides:

- [Commands and differences from Redis](docs/commands.md)
- [Configuration](docs/configuration.md)
- [Persistence and recovery](docs/persistence.md)
- [Replication](docs/replication.md)
- [Limitations and roadmap](docs/limitations.md)

Design and development:

- [Architecture and on-disk format](docs/architecture.md)
- [Benchmarks](docs/benchmarks.md)
- [Contributing](CONTRIBUTING.md)
- [Changelog](CHANGELOG.md)

## Performance

On a 4-core laptop (AMD Ryzen 5 3500U), a single node serves about 395k ops/s on Windows and 1.06M ops/s on Linux (WSL2) for a 90% GET / 10% SET mix with 8 threads. A GET from memory takes about 1 µs. These numbers come from in-process Go benchmarks on one machine; a head-to-head comparison with Redis and replicated-mode numbers for the current Raft engine are not published yet. See [benchmarks](docs/benchmarks.md).

## Contributing

Bug reports and pull requests are welcome. Start with [CONTRIBUTING.md](CONTRIBUTING.md). Report security issues privately as described in [SECURITY.md](SECURITY.md).

## License

Apache License 2.0, see [LICENSE](LICENSE).
