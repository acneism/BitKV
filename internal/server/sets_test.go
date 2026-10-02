package server

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

func members(t *testing.T, c *testConn, args ...string) []string {
	t.Helper()
	got, ok := c.do(args...).([]any)
	if !ok {
		t.Fatalf("%v did not return an array", args)
	}
	out := make([]string, len(got))
	for i, m := range got {
		out[i] = m.(string)
	}
	slices.Sort(out)
	return out
}

func TestSets(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	c := dial(t, addr)
	c.expect(int64(1), "SADD", "myset", "Hello")
	c.expect(int64(1), "SADD", "myset", "World")
	c.expect(int64(0), "SADD", "myset", "World")
	c.expect([]any{"Hello", "World"}, "SMEMBERS", "myset")
	c.expect(int64(3), "SADD", "s", "one", "two", "three")
	c.expect(int64(1), "SREM", "s", "one", "four")
	c.expect(int64(1), "SISMEMBER", "s", "two")
	c.expect(int64(0), "SISMEMBER", "s", "one")
	c.expect([]any{int64(1), int64(0)}, "SMISMEMBER", "s", "two", "nope")
	c.expect(int64(2), "SCARD", "s")
	c.expect(status("set"), "TYPE", "s")
	c.expect("listpack", "OBJECT", "ENCODING", "s")

	c.expect(int64(3), "SADD", "pop", "a", "b", "c")
	if m, _ := c.do("SPOP", "pop").(string); !slices.Contains([]string{"a", "b", "c"}, m) {
		t.Fatalf("SPOP = %q", m)
	}
	if got := members(t, c, "SPOP", "pop", "5"); len(got) != 2 {
		t.Fatalf("SPOP pop 5 = %v", got)
	}
	c.expect(int64(0), "EXISTS", "pop")
	c.expect(nil, "SPOP", "pop")
	c.expect([]any{}, "SPOP", "pop", "1")
	c.expect(errReply("ERR value is out of range, must be positive"), "SPOP", "s", "-1")
	if got, _ := c.do("SRANDMEMBER", "s", "-5").([]any); len(got) != 5 {
		t.Fatalf("SRANDMEMBER s -5 = %v", got)
	}
	if got := members(t, c, "SRANDMEMBER", "s", "5"); !slices.Equal(got, []string{"three", "two"}) {
		t.Fatalf("SRANDMEMBER s 5 = %v", got)
	}

	c.expect(int64(1), "SADD", "other", "x")
	c.expect(int64(1), "SMOVE", "s", "other", "two")
	c.expect(int64(0), "SMOVE", "s", "other", "two")
	c.expect([]any{"three"}, "SMEMBERS", "s")
	c.expect(int64(1), "SMOVE", "s", "s", "three")
	c.expect(status("OK"), "SET", "str", "v")
	c.expect(int64(0), "SMOVE", "missing", "str", "x")
	c.expect(errReply(errWrongType), "SMOVE", "s", "str", "three")

	c.expect(int64(4), "SADD", "key1", "a", "b", "c", "d")
	c.expect(int64(1), "SADD", "key2", "c")
	c.expect(int64(3), "SADD", "key3", "a", "c", "e")
	if got := members(t, c, "SINTER", "key1", "key2", "key3"); !slices.Equal(got, []string{"c"}) {
		t.Fatalf("SINTER = %v", got)
	}
	if got := members(t, c, "SUNION", "key1", "key2", "key3"); !slices.Equal(got, []string{"a", "b", "c", "d", "e"}) {
		t.Fatalf("SUNION = %v", got)
	}
	if got := members(t, c, "SDIFF", "key1", "key2", "key3"); !slices.Equal(got, []string{"b", "d"}) {
		t.Fatalf("SDIFF = %v", got)
	}
	c.expect([]any{}, "SINTER", "key1", "missing")
	c.expect(int64(1), "EXPIRE", "str", "100")
	c.expect(int64(2), "SINTERSTORE", "str", "key1", "key3")
	c.expect(int64(-1), "TTL", "str")
	if got := members(t, c, "SMEMBERS", "str"); !slices.Equal(got, []string{"a", "c"}) {
		t.Fatalf("SINTERSTORE result = %v", got)
	}
	c.expect(int64(0), "SDIFFSTORE", "str", "key2", "key1")
	c.expect(int64(0), "EXISTS", "str")
	c.expect(int64(5), "SUNIONSTORE", "u", "key1", "key3")
	c.expect(int64(2), "SINTERCARD", "2", "key1", "key3")
	c.expect(int64(1), "SINTERCARD", "2", "key1", "key3", "LIMIT", "1")
	c.expect(errReply("ERR numkeys should be greater than 0"), "SINTERCARD", "0", "key1")
	c.expect(errReply("ERR Number of keys can't be greater than number of args"), "SINTERCARD", "3", "key1", "key2")
	c.expect(errReply("ERR LIMIT can't be negative"), "SINTERCARD", "1", "key1", "LIMIT", "-1")
	c.expect([]any{"0", []any{"c"}}, "SSCAN", "key3", "0", "MATCH", "c")
	c.expect(status("OK"), "SET", "str", "v")
	c.expect(errReply(errWrongType), "SADD", "str", "x")
	c.expect(errReply(errWrongType), "SINTER", "key1", "str")

	args := []string{"SADD", "big"}
	for i := range 200 {
		args = append(args, fmt.Sprint(i))
	}
	c.expect(int64(200), args...)
	c.expect("hashtable", "OBJECT", "ENCODING", "big")
	c.expect(int64(1), "SREM", "big", "7")
	c.expect(int64(199), "SCARD", "big")
	c.expect(int64(3), "SADD", "nums", "1", "2", "999")
	c.expect(int64(2), "SINTERCARD", "2", "big", "nums", "LIMIT", "0")
	if got := members(t, c, "SINTER", "nums", "big"); !slices.Equal(got, []string{"1", "2"}) {
		t.Fatalf("SINTER nums big = %v", got)
	}
	stopServer(t, srv, db)

	srv, db, addr = startServer(t, dir)
	defer stopServer(t, srv, db)
	c = dial(t, addr)
	c.expect(int64(199), "SCARD", "big")
	c.expect(int64(0), "SISMEMBER", "big", "7")
	c.expect(int64(1), "SISMEMBER", "big", "199")
	if got := members(t, c, "SMEMBERS", "big"); len(got) != 199 {
		t.Fatalf("SMEMBERS big returned %d members", len(got))
	}
}

func TestSetModel(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	model := map[string]bool{}
	rng := rand.New(rand.NewPCG(3, 4))
	for i := range 4000 {
		m := fmt.Sprint(rng.IntN(200))
		switch rng.IntN(4) {
		case 0, 1:
			c.expect(int64(map[bool]int{true: 0, false: 1}[model[m]]), "SADD", "m", m)
			model[m] = true
		case 2:
			c.expect(int64(map[bool]int{true: 1, false: 0}[model[m]]), "SREM", "m", m)
			delete(model, m)
		default:
			want := slices.Sorted(maps.Keys(model))
			if got := members(t, c, "SMEMBERS", "m"); !slices.Equal(got, want) {
				t.Fatalf("step %d: SMEMBERS = %v, model %v", i, got, want)
			}
			c.expect(int64(len(model)), "SCARD", "m")
		}
	}
}
