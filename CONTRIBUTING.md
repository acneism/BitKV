# Contributing to CasketDB

Thanks for helping. This page explains how to report problems, build and test CasketDB, and what a change needs before it is merged.

## Reporting bugs

Open a GitHub issue with:

- the version (`casketdb_version` in `INFO`) and the commit, if you built from source;
- the OS and the flags you started the server with;
- the smallest sequence of commands that shows the problem, what you expected and what happened;
- the server log around the failure.

Report security problems privately, as described in [SECURITY.md](SECURITY.md), not in an issue.

## Development setup

You need Go 1.26.1 or newer. Nothing else: no cgo and no code generation.

```bash
git clone https://github.com/acneism/casketdb
cd casketdb
go build ./...
go build -o casketdb ./cmd/casketdb
```

The Raft library is a regular module dependency, [github.com/acneism/raft](https://github.com/acneism/raft). To change both at once, use a `go.work` file or a local `replace` directive, and do not commit it.

## Running tests

```bash
go vet ./...
go test ./...
```

Run both on Linux and on Windows when your change touches files, fsync, memory mapping or locking: these paths differ between the two systems.

The race detector needs cgo, so run it on Linux (on Windows, use WSL):

```bash
go test -race ./...
```

To test a Linux build from Windows without installing Go in WSL, cross-compile the test binary and run it there:

```bash
GOOS=linux go test -c -o replica.test ./internal/replica
wsl ./replica.test
```

### Fault-injection tests

`cmd/casketdb` holds end-to-end tests that run real `casketdb` processes: three nodes, eight RESP clients doing SET, GET, INCR, DEL and MULTI/EXEC, and a nemesis. They check the recorded history for linearizability with [Porcupine](https://github.com/anishathalye/porcupine). They are skipped unless a duration is given:

```bash
go test ./cmd/casketdb -run TestFaults -timeout 30m -args -fault.duration=5m
go test ./cmd/casketdb -run TestMembershipChanges -timeout 30m -args -member.duration=5m
```

`TestFaults` kills nodes with `kill -9`, cuts network links between nodes in one or both directions through a proxy, and transfers leadership. `TestMembershipChanges` adds nodes with `-raft-join` and removes voters while the load runs. Other flags: `-fault.reads=lease` runs the nodes with lease reads, `-fault.nosync` with `-raft-unsafe-no-fsync`, `-fault.bin` uses a prebuilt binary, `-fault.out` sets where the Porcupine visualization of a failure goes. Node logs stay in the test's temporary directory, printed on failure.

On Linux without a Go toolchain, cross-compile both the server and the test:

```bash
GOOS=linux go build -o casketdb-linux ./cmd/casketdb
GOOS=linux go test -c -o fault.test ./cmd/casketdb
wsl ./fault.test -test.run TestFaults -test.timeout 30m -fault.duration=5m -fault.bin=./casketdb-linux
```

### Continuous integration

[GitHub Actions](.github/workflows/ci.yml) runs on every push to `main` and every pull request:

- `gofmt` and `go mod tidy -diff`;
- `go vet` and `go test` on Linux, Windows and macOS;
- `go test -race` on Linux;
- one minute of `FuzzReadCommand`.

The [nightly workflow](.github/workflows/nightly.yml), which can also be started by hand, runs `TestFaults` with linearizable reads, `TestFaults` with lease reads and `TestMembershipChanges`, ten minutes each. A failed run keeps the node logs and the Porcupine visualization as build artifacts.

`main` is protected: a commit lands there only after CI has passed on it, on a branch that is up to date with `main`. Push a branch, open a pull request, wait for a green run, then merge.

### Benchmarks

Benchmarks live next to the code, for example:

```bash
go test -run '^$' -bench 'BenchmarkReplicatedSet' ./internal/replica
```

Fsync time varies a lot between runs. When you compare two builds, alternate their runs instead of running one after the other; see [benchmarks](docs/benchmarks.md#methodology-notes).

## Code style

- Code is formatted with `gofmt` and passes `go vet`.
- **No comments in code, tests included.** Names and structure carry the intent. Build constraints such as `//go:build` are not comments and stay.
- Match the surrounding code: naming, error handling, test helpers.
- Code outside `internal/replica` uses only the standard library; the fault-injection tests also use Porcupine. Discuss a new dependency in an issue first.
- Everything that knows about Raft stays in `internal/replica`.

## Tests for a change

- Every behavior change comes with a test that fails without it.
- Use the manual clock from `internal/clock` instead of sleeping on wall time.
- A change to the write path, recovery or merge needs a crash test: a torn tail, a cut batch or a disk image taken at the point of failure. `internal/bitcask/multilog_test.go` has helpers for this.
- A change to the on-disk format must keep opening existing data directories, or bump the format in `META` and migrate.

## Commits and pull requests

- One logical change per commit.
- Subject in the imperative mood, about 72 characters at most, no trailing period: `Track the durable Raft index in Bitcask records`. Explain the why in the body when it is not obvious.
- Open a pull request from a branch. Say what changed and how you tested it: Windows, Linux, `-race`.
- Update the documentation in [docs/](docs/README.md) and the [changelog](CHANGELOG.md) together with the code.

## Where to start reading

[Architecture](docs/architecture.md) describes the packages, the write and read paths, and the on-disk format.
