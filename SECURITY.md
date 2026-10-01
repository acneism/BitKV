# Security policy

## Supported versions

CasketDB has not reached 1.0. Security fixes go to the latest commit on `main` only.

## Reporting a vulnerability

Do not open a public issue. Report the vulnerability privately through GitHub: open the [Security tab](https://github.com/acneism/casketdb/security) of the repository and choose **Report a vulnerability**.

Include the version or commit, the flags the server ran with, steps to reproduce and the impact you expect. The maintainer will confirm the report, work on a fix, and agree on a disclosure date with you. Please keep the details private until a fix is published.

## Security model

Deploy CasketDB with these properties in mind:

- **TLS for clients.** `-tls-addr` serves clients over TLS 1.2 or 1.3; with `-tls-ca`, every client must also present a certificate signed by that CA. Connections to `-addr` are plain text, password included, so on an untrusted network turn it off with `-addr ""` or keep it on loopback. See [TLS for clients](docs/configuration.md#tls-for-clients).
- **Users and permissions.** `-requirepass` sets the password of the `default` user, which may run every command. `ACL SETUSER` creates users limited to command categories, commands and key patterns, with passwords stored as SHA-256 hashes; see [access control](docs/commands.md#access-control). Users are stored in the data directory, in the file `SYSTEM`, and replicated to every node of a cluster. Whoever may run `CONFIG` can change the `default` password and the sync policy; paths, addresses and TLS files cannot be changed at runtime.
- **Loopback and protected mode by default.** The server listens on `127.0.0.1:6379` unless `-addr` says otherwise. Wherever it listens, while the `default` user has no password, protected mode refuses clients from other hosts with `DENIED`, as in Redis; `-protected-mode=false` turns it off. See [protected mode](docs/configuration.md#protected-mode).
- **Password guessing and overload.** After 10 failed `AUTH` attempts from one address, or one IPv6 /64 network, within a second, the server refuses the rest of that second's attempts from it without checking them. Many addresses together can still test many passwords, so use a long random one. `-maxclients` (10,000 by default) caps open connections and `-timeout` closes idle ones; see [client limits](docs/configuration.md#client-limits).
- **Metrics without authentication.** The `/metrics` endpoint at `-metrics-addr` answers anyone who can reach it. It reveals counts and sizes, not keys or values; bind it to a loopback or private address.
- **Password in the environment.** Prefer `CASKETDB_REQUIREPASS` to `-requirepass`: command-line flags are visible in the process list.
- **Raft transport.** Turn on mutual TLS between nodes with `-raft-tls-cert`, `-raft-tls-key` and `-raft-tls-ca`: traffic is encrypted and a node must present a certificate for its own id. Without TLS, nodes trust any peer that connects to the Raft port, and the server logs a warning when its Raft address is not a loopback address. Either way, firewall Raft ports from clients. See [mutual TLS between nodes](docs/replication.md#mutual-tls-between-nodes).
- **Private files.** CasketDB creates its directories with mode `0700` and its files with `0600`, so other users of the server cannot read the data. It does not change the mode of an existing directory; if `-dir` or the Raft directory is open to other users, the server logs a warning at start. On Windows, access follows the ACLs inherited from the parent directory.
- **No encryption at rest.** Data files hold keys and values as written. Protect the data directory with file-system permissions or disk encryption.
- **Protocol limits.** A bulk string is limited by `-proto-max-bulk-len` (512 MB by default), a command by 1,048,576 arguments and an inline command by 64 KB. Large bulk strings are read as they arrive, never preallocated from the declared size.
