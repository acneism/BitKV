# Security policy

## Supported versions

CasketDB has not reached 1.0. Security fixes go to the latest commit on `main` only.

## Reporting a vulnerability

Do not open a public issue. Report the vulnerability privately through GitHub: open the [Security tab](https://github.com/acneism/casketdb/security) of the repository and choose **Report a vulnerability**.

Include the version or commit, the flags the server ran with, steps to reproduce and the impact you expect. The maintainer will confirm the report, work on a fix, and agree on a disclosure date with you. Please keep the details private until a fix is published.

## Security model

Deploy CasketDB with these properties in mind:

- **No TLS.** Client connections, including the password, travel in plain text. Use a private network, a VPN or a TLS-terminating proxy.
- **One password, no users.** `-requirepass` protects every command except AUTH, HELLO and QUIT. There are no per-user permissions.
- **Loopback by default.** The server listens on `127.0.0.1:6379` unless `-addr` says otherwise, and logs a warning if it listens elsewhere without a password.
- **Password in the environment.** Prefer `CASKETDB_REQUIREPASS` to `-requirepass`: command-line flags are visible in the process list.
- **Unauthenticated Raft transport.** Cluster nodes trust any peer that connects to the Raft port. Keep Raft ports on a trusted network and firewall them from clients.
- **No encryption at rest.** Data files hold keys and values as written. Protect the data directory with file-system permissions or disk encryption.
- **Protocol limits.** A bulk string is limited by `-proto-max-bulk-len` (512 MB by default), a command by 1,048,576 arguments and an inline command by 64 KB. Large bulk strings are read as they arrive, never preallocated from the declared size.
