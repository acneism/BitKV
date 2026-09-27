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

Benchmarks live next to the code, for example:

```bash
go test -run '^$' -bench 'BenchmarkReplicatedSet' ./internal/replica
```

Fsync time varies a lot between runs. When you compare two builds, alternate their runs instead of running one after the other; see [benchmarks](docs/benchmarks.md#methodology-notes).

## Code style

- Code is formatted with `gofmt` and passes `go vet`.
- **No comments in code, tests included.** Names and structure carry the intent. Build constraints such as `//go:build` are not comments and stay.
- Match the surrounding code: naming, error handling, test helpers.
- Packages other than `internal/replica` use only the standard library. Discuss a new dependency in an issue first.
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
