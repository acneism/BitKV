# Commands and differences from Redis

CasketDB implements the string subset of Redis 7 over RESP2. Semantics, replies and error texts follow Redis. An unknown command returns `-ERR unknown command`, a wrong argument count returns `-ERR wrong number of arguments`, and the connection stays open in both cases.

## Supported commands

| Group | Commands | Notes |
| --- | --- | --- |
| Strings | GET, SET, SETNX, SETEX, PSETEX, GETSET, GETEX, GETDEL, MGET, MSET, MSETNX, APPEND, STRLEN, GETRANGE, SETRANGE, INCR, DECR, INCRBY, DECRBY, INCRBYFLOAT, LCS | SET accepts EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET; GETEX accepts EX, PX, EXAT, PXAT, PERSIST; LCS accepts LEN, IDX, MINMATCHLEN, WITHMATCHLEN. MSET, MSETNX and INCR* are atomic |
| Hashes | HSET, HMSET, HSETNX, HGET, HMGET, HDEL, HLEN, HEXISTS, HSTRLEN, HGETALL, HKEYS, HVALS, HINCRBY, HINCRBYFLOAT, HSCAN, HRANDFIELD | See [hashes](#hashes) |
| Sets | SADD, SREM, SISMEMBER, SMISMEMBER, SMEMBERS, SCARD, SPOP, SRANDMEMBER, SMOVE, SINTER, SINTERSTORE, SINTERCARD, SUNION, SUNIONSTORE, SDIFF, SDIFFSTORE, SSCAN | See [sets](#sets) |
| Bitmaps | SETBIT, GETBIT, BITCOUNT, BITPOS, BITOP, BITFIELD, BITFIELD_RO | Bitmaps are strings. BITCOUNT and BITPOS accept BYTE and BIT ranges; BITOP supports AND, OR, XOR and NOT; BITFIELD supports GET, SET, INCRBY and OVERFLOW WRAP, SAT or FAIL. A bit offset is below 2³² |
| Keys | DEL, UNLINK, EXISTS, TYPE, OBJECT, KEYS, SCAN, DBSIZE | Glob patterns `*`, `?`, `[a-z]`, `[^x]`, `\`. SCAN accepts MATCH, COUNT, TYPE. OBJECT supports ENCODING only: `int`, `embstr` or `raw` for a string, as in Redis |
| Expiry | EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST | NX, XX, GT, LT. A time in the past deletes the key. TTL returns −2 for a missing key and −1 for a key without expiry |
| Transactions | MULTI, EXEC, DISCARD, WATCH, UNWATCH | See [transactions](#transactions) |
| Connection | PING, ECHO, QUIT, AUTH, SELECT, HELLO, CLIENT | CLIENT supports ID, GETNAME, SETNAME, SETINFO |
| Access control | ACL SETUSER, ACL GETUSER, ACL DELUSER, ACL LIST, ACL USERS, ACL WHOAMI, ACL CAT, ACL LOG | See [access control](#access-control) |
| Server | INFO, FLUSHDB, FLUSHALL, SAVE, BGREWRITEAOF, COMMAND, CONFIG | See [server commands](#server-commands) |
| Cluster | RAFT MEMBERS, RAFT ADDLEARNER, RAFT PROMOTE, RAFT REMOVE, RAFT TRANSFER | CasketDB's own commands, see [cluster administration](#cluster-administration) |

## Differences from Redis

### Data types

Strings, including the bitmap commands, hashes and sets. Lists, sorted sets, streams, HyperLogLog, geo, pub/sub, Lua and Functions are not implemented.

### Hashes

A hash has two encodings, with the thresholds of Redis:

- Up to 128 fields, none of them and none of their values longer than 64 bytes, the hash is one value, its fields in the order they were added. `OBJECT ENCODING` answers `listpack`. A change rewrites the whole hash.
- Beyond that, each field is a record of its own, and a change writes only the fields it touches plus a small record with the field count. `OBJECT ENCODING` answers `hashtable`. Like Redis, a hash never goes back to `listpack`, even when it shrinks.

Differences from Redis:

- HSCAN returns every matching field in one reply with cursor `0`, for both encodings, and accepts `NOVALUES`. COUNT is checked but does not split the reply.
- HRANDFIELD reads every field of the hash, and with a count accepts at most 16,777,216 fields either way; Redis has no such limit.
- Field expiry (HEXPIRE and the other commands of Redis 7.4) is not supported.

### Sets

A set uses the same two encodings as a hash, with the same thresholds: up to 128 members of up to 64 bytes it is one value (`listpack`), beyond that each member is a record of its own (`hashtable`). There is no `intset` encoding for small sets of integers. SSCAN returns every matching member in one reply with cursor `0`; SPOP and SRANDMEMBER read every member of the set, and SRANDMEMBER with a count accepts at most 16,777,216 members either way. `TYPE` returns `string` or `none`.

### Numbers and string sizes

INCR, DECR, INCRBY and DECRBY accept a strict 64-bit integer: no leading `+`, no leading zeros. Overflow returns an error, as in Redis.

INCRBYFLOAT computes with 64-bit floating point and stores the shortest decimal form of the result. Redis computes with `long double`, so the last digits of a result can differ: `0.1` plus `0.2` is `0.30000000000000004` here and `0.3` in Redis.

APPEND, SETRANGE and LCS limit a string, or the memory LCS needs, to 512 MB, the default `proto-max-bulk-len` of Redis, whatever `-proto-max-bulk-len` is set to.

### SCAN

The cursor is the number of a keydir shard (0–1023), not a position in a hash table. A full iteration visits every shard once, so it returns every key that existed for the whole scan, like Redis. COUNT (10 by default) is how many keys to examine; a call always finishes the shard it started, so a reply may hold more keys than COUNT.

Outside MULTI, KEYS, SCAN and DBSIZE lock one shard at a time. The result is not a point-in-time snapshot: keys written during the call may or may not appear.

### Transactions

- EXEC runs the queued commands in one transaction and writes them as one atomic batch.
- A queued command with an unknown name or a wrong argument count aborts EXEC with `EXECABORT`, as in Redis.
- WATCH compares the position of a key's last write. Expiry of a watched key counts as a change.
- WATCH can fire spuriously after a background merge moves a key, and it does not notice a key that was created and deleted between WATCH and EXEC.
- Connection and server commands (SELECT, INFO, CONFIG, FLUSHDB and others) are rejected inside MULTI with `ERR Command not allowed inside a transaction`.

### Connection

- Only RESP2. `HELLO 3` returns `-NOPROTO`; `HELLO 2` accepts `AUTH` and `SETNAME`.
- `AUTH <password>` signs in as `default`, `AUTH <user> <password>` as any user. Until a client authenticates, every command except AUTH, HELLO and QUIT returns `NOAUTH`. When `default` has no password, a new connection is signed in as `default` right away; in [protected mode](configuration.md#protected-mode), on by default, a client from another host gets `DENIED` instead and is disconnected.
- After 10 failed `AUTH` attempts from one address within a second, `AUTH` and `HELLO … AUTH` from that address answer `ERR too many failed AUTH attempts` until the second is over, even with the right password. Redis has no such limit; see [client limits](configuration.md#client-limits).
- Only database 0. `SELECT 0` succeeds; any other index returns an error.

### Access control

Users work as in Redis 6 and later. `default` always exists; `CONFIG SET requirepass` sets its password, and so does `-requirepass` until the first change to users is stored. Passwords are stored as SHA-256 hashes.

Users are kept in the file `SYSTEM` in the data directory and survive restarts; once it exists, `-requirepass` is ignored at start, with a warning. In a cluster a change to users is a Raft entry: run it on the leader, a follower answers `READONLY`, and every node applies it and includes it in snapshots. FLUSHDB does not touch users.

`ACL SETUSER` understands `on`, `off`, `>password`, `<password`, `#hash`, `!hash`, `nopass`, `resetpass`, `~pattern`, `allkeys`, `resetkeys`, `+command`, `-command`, `+@category`, `-@category`, `allcommands`, `nocommands` and `reset`. A new user starts `off`, without passwords, keys or commands. The categories are `keyspace`, `read`, `write`, `string`, `bitmap`, `hash`, `set`, `fast`, `slow`, `admin`, `dangerous`, `connection` and `transaction`, assigned as in Redis; `RAFT` is `@admin` and `@dangerous`. A denied command answers `NOPERM`, and inside MULTI it aborts EXEC. `ACL LOG [count|RESET]` lists the latest denials of commands, keys and logins on this node, newest first, up to 128; a repeat within a minute adds to the count of its entry.

Differences from Redis:

- Read-only and write-only key patterns (`%R~`, `%W~`), channels (`&`), selectors and rules for single subcommands (`+config|get`) are not supported.
- User names and key patterns must be valid UTF-8, because `SYSTEM` is JSON; Redis takes any bytes in a key pattern.
- `ACL SAVE`, `ACL LOAD`, `ACL GENPASS` and `ACL DRYRUN` are missing.
- `ACL WHOAMI` and `ACL CAT` are open to every authenticated user; the other ACL subcommands need the `acl` command.
- Disabling a user with `off` stops new logins; open connections keep working. Deleting a user closes its connections at their next command.

### Server commands

- `SAVE` forces an fsync of all logs. There is no RDB file.
- `BGREWRITEAOF` starts a merge (compaction) of all logs.
- `CONFIG GET` answers `appendonly`, `appendfsync`, `save`, `databases`, `proto-max-bulk-len` and `requirepass`.
- `CONFIG SET` changes `requirepass`, `appendfsync` and `proto-max-bulk-len`, several at once and all or none. `requirepass` is the password of the `default` user: it is stored and, in a cluster, replicated like other changes to users. `appendfsync` and `proto-max-bulk-len` change only the node it runs on, as in Redis, until restart; there is no config file for `CONFIG REWRITE` to write, so keep them in [flags or `CASKETDB_` variables](configuration.md). A new password does not log out open connections, and a new `proto-max-bulk-len` applies to new connections.
- `COMMAND` returns an empty list and `COMMAND COUNT` the number of commands. Both exist so that `redis-cli` and `redis-benchmark` start.
- `FLUSHDB` and `FLUSHALL` do the same thing.

### Expiry

Expiry is exact to the millisecond. Keys expire lazily on access and actively in the background: every 100 ms a cursor walks the shards, and a full pass takes at most about 6.4 s. In a cluster each node expires keys by its own clock, see [replication](replication.md#expiry).

### Cluster mode

In a cluster only the leader accepts writes. Followers answer `-READONLY You can't write against a read only replica.`, as a Redis replica does. There is no Redis Cluster protocol (`CLUSTER`, `MOVED`, `ASK`): every node holds all keys.

With consistent reads turned on, a read that no leader could confirm returns `-TRYAGAIN No leader confirmed the read, retry.` The read had no effect, so retrying is safe. See [consistent reads](replication.md#consistent-reads).

### Cluster administration

`RAFT` groups the commands that manage a CasketDB cluster. Redis has no equivalent: it is not the Redis Cluster protocol. The commands need a cluster node (`-raft-id`) and are rejected inside MULTI. All but `RAFT MEMBERS` must run on the leader.

| Command | Reply | Effect |
| --- | --- | --- |
| `RAFT MEMBERS` | Array of `[id, raft address, voter\|learner]` | The membership as this node knows it |
| `RAFT ADDLEARNER id addr` | `OK` | Adds a node that was started with `-raft-join` as a learner |
| `RAFT PROMOTE id` | `OK` | Waits until the learner has caught up, then makes it a voter |
| `RAFT REMOVE id` | `OK` | Removes a voter or a learner; stop the removed node afterwards |
| `RAFT TRANSFER [id]` | `OK` | Hands leadership to the node `id`, or to any other voter. See [leadership transfer](replication.md#leadership-transfer) |

See [changing membership](replication.md#changing-membership) for the procedure.

On a node that is not the leader, the commands answer `ERR this node is not the leader, run it on <id> (raft address <addr>)`.

## Limits

| Limit | Value |
| --- | --- |
| Bulk string (key or value) | `-proto-max-bulk-len`, 512 MB by default |
| Arguments per command | 1,048,576 |
| Inline command line | 64 KB |
| Command from a client that has not authenticated | 10 arguments of up to 16 KB, as in Redis; more closes the connection with `ERR Protocol error: unauthenticated …` |
