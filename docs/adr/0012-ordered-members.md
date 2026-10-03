# 12. An ordered index of members in the engine

Status: accepted, 2026-10-03. Extends [ADR 11](0011-collection-members.md) for sorted sets.

## Context

A sorted set answers by rank and by range: ZRANK, ZRANGE, ZCOUNT, ZPOPMIN. A large one keeps its members in records of their own (ADR 11), and their table in memory is a map with no order. Sorting the members for every such command costs O(n log n) per command, and Redis answers these in O(log n) with a skiplist. The order must survive restarts and be the same on every node, and a transaction must see its own changes and, on a Raft leader, the changes proposed in its term.

## Decision

A collection key whose type byte has bit 0x40 set, next to the table bit, keeps its members in a skiplist as well as in the map. The skiplist is a port of the Redis one: nodes carry spans, so rank and position are found in O(log n). Members are ordered by their values as bytes and then by member; a sorted set stores a score in a form whose byte order is the numeric order. The engine updates the skiplist where it updates the table: when a transaction applies, and at the start, where the loader reads the value of each member of an ordered table.

A transaction reads the order through a view: the stored skiplist, minus the members the transaction or a proposal of its term changed, plus their new values. Counting and positioning in the view cost O(log n) for the stored skiplist and O(k log n + k²) for k changed members, so a sorted set that is changing all the time on a leader is not sorted again for each read.

## Consequences

- Rank, count and range on a large sorted set cost O(log n), plus the members returned.
- A member of an ordered table costs a skiplist node and a copy of its value in memory, on top of what ADR 11 costs.
- The start of a database reads the value of every member of an ordered table, one read per member, because hint files carry no values.
- Other collections keep the map only.
