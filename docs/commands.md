# Commands and differences from Redis

CasketDB implements the string subset of Redis 7 over RESP2. Semantics, replies and error texts follow Redis. An unknown command returns `-ERR unknown command`, a wrong argument count returns `-ERR wrong number of arguments`, and the connection stays open in both cases.

## Supported commands

| Group | Commands | Notes |
| --- | --- | --- |
| Strings | GET, SET, SETNX, SETEX, PSETEX, GETDEL, MGET, MSET, APPEND, STRLEN, INCR, DECR, INCRBY, DECRBY | SET accepts EX, PX, EXAT, PXAT, NX, XX, KEEPTTL, GET. MSET and INCR* are atomic |
| Keys | DEL, UNLINK, EXISTS, TYPE, KEYS, SCAN, DBSIZE | Glob patterns `*`, `?`, `[a-z]`, `[^x]`, `\`. SCAN accepts MATCH, COUNT, TYPE |
| Expiry | EXPIRE, PEXPIRE, EXPIREAT, PEXPIREAT, TTL, PTTL, PERSIST | NX, XX, GT, LT. A time in the past deletes the key. TTL returns −2 for a missing key and −1 for a key without expiry |
| Transactions | MULTI, EXEC, DISCARD, WATCH, UNWATCH | See [transactions](#transactions) |
| Connection | PING, ECHO, QUIT, AUTH, SELECT, HELLO, CLIENT | CLIENT supports ID, GETNAME, SETNAME, SETINFO |
| Server | INFO, FLUSHDB, FLUSHALL, SAVE, BGREWRITEAOF, COMMAND, CONFIG | See [server commands](#server-commands) |

## Differences from Redis

### Data types

Only strings. Lists, hashes, sets, sorted sets, streams, bitmaps, HyperLogLog, geo, pub/sub, Lua and Functions are not implemented. `TYPE` returns `string` or `none`.

### Integers

INCR, DECR, INCRBY and DECRBY accept a strict 64-bit integer: no leading `+`, no leading zeros. Overflow returns an error, as in Redis.

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
- One user. `AUTH <password>` and `AUTH default <password>` are accepted. Until a client authenticates, every command except AUTH, HELLO and QUIT returns `NOAUTH`.
- Only database 0. `SELECT 0` succeeds; any other index returns an error.

### Server commands

- `SAVE` forces an fsync of all logs. There is no RDB file.
- `BGREWRITEAOF` starts a merge (compaction) of all logs.
- `CONFIG GET` answers `appendonly`, `appendfsync`, `save`, `databases` and `proto-max-bulk-len`. `CONFIG SET` is not supported; configure the server with [flags](configuration.md).
- `COMMAND` returns an empty list and `COMMAND COUNT` the number of commands. Both exist so that `redis-cli` and `redis-benchmark` start.
- `FLUSHDB` and `FLUSHALL` do the same thing.

### Expiry

Expiry is exact to the millisecond. Keys expire lazily on access and actively in the background: every 100 ms a cursor walks the shards, and a full pass takes at most about 6.4 s. In a cluster each node expires keys by its own clock, see [replication](replication.md#expiry).

### Cluster mode

In a cluster only the leader accepts writes. Followers answer `-READONLY You can't write against a read only replica.`, as a Redis replica does. There is no Redis Cluster protocol (`CLUSTER`, `MOVED`, `ASK`): every node holds all keys.

With consistent reads turned on, a read that no leader could confirm returns `-TRYAGAIN No leader confirmed the read, retry.` The read had no effect, so retrying is safe. See [consistent reads](replication.md#consistent-reads).

## Limits

| Limit | Value |
| --- | --- |
| Bulk string (key or value) | `-proto-max-bulk-len`, 512 MB by default |
| Arguments per command | 1,048,576 |
| Inline command line | 64 KB |
