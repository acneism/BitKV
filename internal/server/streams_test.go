package server

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"
)

func entry(id string, fields ...string) []any {
	return []any{id, strs(fields...)}
}

func TestStreams(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	a, c := dial(t, addr), dial(t, addr)
	c.expect("1-1", "XADD", "s", "1-1", "a", "1")
	c.expect(errReply("ERR The ID specified in XADD is equal or smaller than the target stream top item"), "XADD", "s", "1-1", "b", "2")
	c.expect(errReply("ERR The ID specified in XADD must be greater than 0-0"), "XADD", "s", "0-0", "b", "2")
	c.expect("1-2", "XADD", "s", "1-*", "b", "2")
	c.expect("2-0", "XADD", "s", "2", "c", "3", "d", "4")
	auto, _ := c.do("XADD", "s", "*", "e", "5").(string)
	if ms, err := strconv.ParseUint(strings.TrimSuffix(auto, "-0"), 10, 64); err != nil || ms < 2 {
		t.Fatalf("XADD * = %q", auto)
	}
	c.expect(int64(4), "XLEN", "s")
	c.expect(status("stream"), "TYPE", "s")
	c.expect("stream", "OBJECT", "ENCODING", "s")
	c.expect([]any{entry("1-1", "a", "1"), entry("1-2", "b", "2"), entry("2-0", "c", "3", "d", "4"), entry(auto, "e", "5")}, "XRANGE", "s", "-", "+")
	c.expect([]any{entry("1-1", "a", "1"), entry("1-2", "b", "2")}, "XRANGE", "s", "-", "+", "COUNT", "2")
	c.expect([]any{entry("1-1", "a", "1"), entry("1-2", "b", "2")}, "XRANGE", "s", "1", "1")
	c.expect([]any{entry("1-2", "b", "2"), entry("2-0", "c", "3", "d", "4")}, "XRANGE", "s", "(1-1", "2")
	c.expect([]any{entry(auto, "e", "5")}, "XREVRANGE", "s", "+", "-", "COUNT", "1")
	c.expect([]any{entry("2-0", "c", "3", "d", "4"), entry("1-2", "b", "2")}, "XREVRANGE", "s", "(3", "1-2")
	c.expect(nil, "XRANGE", "s", "-", "+", "COUNT", "0")
	c.expect([]any{}, "XRANGE", "missing", "-", "+")
	c.expect(errReply("ERR invalid end ID for the interval"), "XRANGE", "s", "-", "(0-0")
	c.expect(errReply(errStreamID), "XRANGE", "s", "x", "+")
	c.expect(int64(1), "XDEL", "s", "1-2", "9-9")
	c.expect(int64(3), "XLEN", "s")
	info, _ := c.do("XINFO", "STREAM", "s").([]any)
	want := []any{"length", int64(3), "radix-tree-keys", int64(0), "radix-tree-nodes", int64(0), "last-generated-id", auto,
		"max-deleted-entry-id", "1-2", "entries-added", int64(4), "recorded-first-entry-id", "1-1", "groups", int64(0),
		"first-entry", entry("1-1", "a", "1"), "last-entry", entry(auto, "e", "5")}
	if fmt.Sprint(info) != fmt.Sprint(want) {
		t.Fatalf("XINFO STREAM s = %v", info)
	}
	c.expect(int64(1), "XTRIM", "s", "MAXLEN", "2")
	c.expect([]any{entry("2-0", "c", "3", "d", "4"), entry(auto, "e", "5")}, "XRANGE", "s", "-", "+")
	c.expect(int64(1), "XTRIM", "s", "MINID", "=", "3")
	c.expect(int64(0), "XTRIM", "s", "MAXLEN", "~", "5", "LIMIT", "10")
	c.expect(int64(1), "XLEN", "s")
	c.expect(nil, "XADD", "nomk", "NOMKSTREAM", "*", "f", "v")
	c.expect(int64(0), "EXISTS", "nomk")
	c.expect("1-1", "XADD", "e", "MAXLEN", "0", "1-1", "f", "v")
	c.expect(int64(0), "XLEN", "e")
	c.expect(int64(1), "EXISTS", "e")
	c.expect(status("OK"), "XSETID", "e", "5-5")
	c.expect(errReply("ERR The ID specified in XADD is equal or smaller"), "XADD", "e", "5-5", "f", "v")
	c.expect("5-6", "XADD", "e", "5-*", "f", "v")
	c.expect(errReply("ERR The ID specified in XSETID is smaller than the target stream top item"), "XSETID", "e", "5-5")
	c.expect(errReply(errNoKey), "XSETID", "missing", "1-1")

	c.expect(errReply("ERR The MAXLEN argument must be >= 0."), "XADD", "s", "MAXLEN", "-1", "*", "f", "v")
	c.expect(errReply("ERR syntax error, LIMIT cannot be used without specifying a trimming strategy"), "XADD", "s", "LIMIT", "10", "*", "f", "v")
	c.expect(errReply("ERR syntax error, LIMIT cannot be used without the special ~ option"), "XADD", "s", "MAXLEN", "1", "LIMIT", "10", "*", "f", "v")
	c.expect(errReply("ERR syntax error, MAXLEN and MINID options at the same time are not compatible"), "XADD", "s", "MAXLEN", "1", "MINID", "1", "*", "f", "v")
	c.expect(errReply("ERR wrong number of arguments for 'xadd' command"), "XADD", "s", "*", "f", "v", "g")
	c.expect(errReply(errStreamID), "XADD", "s", "abc", "f", "v")
	c.expect(errReply(errSyntax), "XTRIM", "s", "FOO", "1")
	c.expect(status("OK"), "SET", "str", "v")
	c.expect(errReply(errWrongType), "XADD", "str", "*", "f", "v")
	c.expect(errReply(errWrongType), "XRANGE", "str", "-", "+")

	c.expect("1-0", "XADD", "r", "1-0", "x", "1")
	c.expect("2-0", "XADD", "r", "2-0", "x", "2")
	c.expect([]any{[]any{"r", []any{entry("1-0", "x", "1")}}}, "XREAD", "COUNT", "1", "STREAMS", "r", "0")
	c.expect([]any{[]any{"r", []any{entry("2-0", "x", "2")}}}, "XREAD", "STREAMS", "r", "missing", "1-0", "0")
	c.expect(nil, "XREAD", "STREAMS", "r", "$")
	c.expect(errReply("ERR Unbalanced 'xread' list of streams"), "XREAD", "STREAMS", "r", "s", "0")
	c.expect(errReply("ERR The > ID can be specified only when calling XREADGROUP"), "XREAD", "STREAMS", "r", ">")
	a.send("XREAD", "BLOCK", "0", "STREAMS", "r", "$")
	waitBlocked(t, c, 1)
	c.expect("3-0", "XADD", "r", "3-0", "x", "3")
	a.expectRead([]any{[]any{"r", []any{entry("3-0", "x", "3")}}})
	start := time.Now()
	c.expect(nil, "XREAD", "BLOCK", "50", "STREAMS", "r", "$")
	if time.Since(start) < 50*time.Millisecond {
		t.Fatal("XREAD BLOCK 50 returned early")
	}
	c.expect(status("OK"), "MULTI")
	c.expect(status("QUEUED"), "XREAD", "BLOCK", "0", "STREAMS", "r", "$")
	c.expect([]any{nil}, "EXEC")

	for i := range 300 {
		c.expect(fmt.Sprintf("%d-0", i+1), "XADD", "big", strconv.Itoa(i+1), "n", strconv.Itoa(i))
	}
	c.expect(int64(300), "XLEN", "big")
	c.expect(int64(50), "XTRIM", "big", "MAXLEN", "250")
	c.expect([]any{entry("51-0", "n", "50")}, "XRANGE", "big", "-", "+", "COUNT", "1")
	check := func() {
		t.Helper()
		c := dial(t, addr)
		c.expect(int64(250), "XLEN", "big")
		c.expect([]any{entry("300-0", "n", "299"), entry("299-0", "n", "298")}, "XREVRANGE", "big", "+", "-", "COUNT", "2")
		c.expect([]any{entry("100-0", "n", "99")}, "XRANGE", "big", "100", "100")
		c.expect([]any{entry(auto, "e", "5")}, "XRANGE", "s", "-", "+")
	}
	check()
	stopServer(t, srv, db)
	srv, db, addr = startServer(t, dir)
	check()
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	stopServer(t, srv, db)
	srv, db, addr = startServer(t, dir)
	defer stopServer(t, srv, db)
	check()
}

func TestStreamModel(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	var model []int
	next := 1
	rng := rand.New(rand.NewPCG(13, 14))
	ids := func(from, to int, count int, reverse bool) []any {
		out := []any{}
		for _, id := range model {
			if id >= from && id <= to {
				out = append(out, entry(fmt.Sprintf("%d-0", id), "v", strconv.Itoa(id)))
			}
		}
		if reverse {
			slices.Reverse(out)
		}
		if count > 0 && len(out) > count {
			out = out[:count]
		}
		return out
	}
	for range 3000 {
		switch op := rng.IntN(10); {
		case op < 5:
			next += 1 + rng.IntN(3)
			model = append(model, next)
			c.expect(fmt.Sprintf("%d-0", next), "XADD", "m", fmt.Sprintf("%d-0", next), "v", strconv.Itoa(next))
		case op == 5 && len(model) > 0:
			id := model[rng.IntN(len(model))]
			model = slices.DeleteFunc(model, func(x int) bool { return x == id })
			c.expect(int64(1), "XDEL", "m", fmt.Sprintf("%d-0", id))
		case op == 6 && len(model) > 140:
			n := len(model) - 120
			model = model[n:]
			c.expect(int64(n), "XTRIM", "m", "MAXLEN", "120")
		default:
			from, to := rng.IntN(next+2), rng.IntN(next+2)
			count := 1 + rng.IntN(5)
			c.expect(ids(from, to, count, false), "XRANGE", "m", strconv.Itoa(from), strconv.Itoa(to), "COUNT", strconv.Itoa(count))
			c.expect(ids(from, to, count, true), "XREVRANGE", "m", strconv.Itoa(to), strconv.Itoa(from), "COUNT", strconv.Itoa(count))
			c.expect(int64(len(model)), "XLEN", "m")
		}
	}
}
