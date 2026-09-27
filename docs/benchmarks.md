# Benchmarks

All numbers below were measured on one machine: AMD Ryzen 5 3500U (4 cores, 8 threads), Windows 10 and Linux under WSL2 on the same laptop. They come from `go test -bench` in-process benchmarks unless stated otherwise. They show the effect of CasketDB's own changes; they are not a comparison with other databases.

A head-to-head comparison with Redis (`redis-benchmark` on the same hardware) and replicated-mode numbers for the current Raft engine (v0.10) are not published yet.

## Throughput, single node

Thousands of operations per second, 8 threads unless stated otherwise, median of 3 runs, v0.2 → v0.3 → v0.4. v0.3 replaced the global lock with 1024 shards; v0.4 added parallel logs.

| Scenario | Windows | Linux (WSL2) |
| --- | --- | --- |
| 90% GET / 10% SET | 104 → 177 → 395 | 206 → 791 → 1,060 |
| SET, distinct keys, 1 / 4 / 8 logs (v0.4) | 224 / 314 / 272 | 251 / 365 / 240 |
| INCR | 55 → 196 → 205 | 126 → 176 → 186 |
| MSET of two keys in different logs (v0.4) | 104 | 50–160 (high variance) |
| Engine PUT with `appendfsync always`, 128 writers | 59 → 67 → 58 | 19 → 18 → 16 |
| SET over TCP, 8 connections, pipeline of 64 | 121 → 136 → 142 | 127 → 155 → 153 |

Four logs are the best choice on 4 cores: eight add system calls and context switches. With `always`, several logs are slightly slower, because each log fsyncs separately and each fsync covers fewer writes.

v0.6 executes consecutive pipelined writes of one connection as one transaction. SET over TCP with a pipeline of 64, without replication, went from 298k to 472k ops/s on Windows.

## Read latency

Nanoseconds per operation, v0.7 → v0.8. v0.8 serves reads from memory-mapped files and an in-memory copy of the active file instead of a system call per read.

| Scenario | Windows | Linux (WSL2) |
| --- | --- | --- |
| GET, 1 thread | 7,000 → 1,000 | 2,540 → 1,220 |
| GET, 8 threads | 2,250 → 230 | 830 → 380 |
| 90% GET / 10% SET, 8 threads | 2,430 → 700 | 1,130 → 920 |
| GET over TCP, pipelined | 6,620 → 2,800 | 3,800 → 2,490 |

On Windows the `ReadFile` system call took about 70% of a GET, so the gain is larger there than on Linux, where `pread` is cheaper.

## Allocations

v0.9 cut allocations per command, test client included: SET 10 → 6, GET 12 → 7, INCR 9 → 6. Time per operation stayed within noise; the gain is less garbage-collection work.

## Methodology notes

- Fsync time on this machine is bimodal on Windows and varies by 10–30% between runs. Compare two builds with interleaved runs, never runs taken hours apart.
- WSL2 fsyncs a virtual disk, about 2.5 ms per call, so fsync-bound numbers on Linux here are pessimistic compared to a server with NVMe.
- Latency percentiles are only meaningful on Linux: the Windows timer is too coarse.
