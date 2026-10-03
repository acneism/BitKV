package server

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func TestSortedSets(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.expect(int64(1), "ZADD", "myzset", "1", "one")
	c.expect(int64(1), "ZADD", "myzset", "1", "uno")
	c.expect(int64(2), "ZADD", "myzset", "2", "two", "3", "three")
	c.expect(strs("one", "1", "uno", "1", "two", "2", "three", "3"), "ZRANGE", "myzset", "0", "-1", "WITHSCORES")
	c.expect(int64(4), "ZCARD", "myzset")
	c.expect(status("zset"), "TYPE", "myzset")
	c.expect("listpack", "OBJECT", "ENCODING", "myzset")
	c.expect(int64(4), "ZCOUNT", "myzset", "-inf", "+inf")
	c.expect(int64(2), "ZCOUNT", "myzset", "(1", "3")
	c.expect("3", "ZINCRBY", "myzset", "2", "one")
	c.expect("3", "ZSCORE", "myzset", "one")
	c.expect([]any{"3", nil}, "ZMSCORE", "myzset", "one", "nofield")
	c.expect(int64(3), "ZRANK", "myzset", "three")
	c.expect(int64(2), "ZRANK", "myzset", "one")
	c.expect(int64(3), "ZREVRANK", "myzset", "uno")
	c.expect(nil, "ZRANK", "myzset", "nofield")
	c.expect([]any{int64(3), "3"}, "ZRANK", "myzset", "three", "WITHSCORE")
	c.expect(nil, "ZRANK", "myzset", "nofield", "WITHSCORE")
	c.expect(strs("one"), "ZRANGE", "myzset", "(1", "+inf", "BYSCORE", "LIMIT", "1", "1")
	c.expect(strs("three", "one", "two"), "ZRANGE", "myzset", "3", "2", "BYSCORE", "REV")
	c.expect(strs("three", "one"), "ZREVRANGE", "myzset", "0", "1")
	c.expect(strs("uno", "1", "two", "2"), "ZRANGEBYSCORE", "myzset", "-inf", "+inf", "WITHSCORES", "LIMIT", "0", "2")
	c.expect(strs("three", "one"), "ZREVRANGEBYSCORE", "myzset", "+inf", "(2")
	c.expect(strs(), "ZRANGEBYSCORE", "myzset", "-inf", "+inf", "LIMIT", "-1", "2")
	c.expect(strs("two", "one", "three"), "ZRANGEBYSCORE", "myzset", "(1", "+inf", "LIMIT", "0", "-1")
	c.expect(strs("uno"), "ZRANGE", "myzset", "-10", "0")
	c.expect(strs(), "ZRANGE", "myzset", "5", "10")
	c.expect(errReply("ERR syntax error, LIMIT is only supported"), "ZRANGE", "myzset", "0", "-1", "LIMIT", "0", "1")
	c.expect(errReply("ERR syntax error, WITHSCORES not supported"), "ZRANGE", "myzset", "-", "+", "BYLEX", "WITHSCORES")
	c.expect(errReply("ERR min or max is not a float"), "ZRANGEBYSCORE", "myzset", "a", "b")
	c.expect(errReply(errSyntax), "ZRANGE", "myzset", "0", "-1", "REV", "REV")
	c.expect(errReply(errSyntax), "ZRANGEBYSCORE", "myzset", "0", "1", "REV")
	c.expect(errReply(errNotInteger), "ZRANGE", "myzset", "a", "-1")

	c.expect(int64(7), "ZADD", "lex", "0", "a", "0", "b", "0", "c", "0", "d", "0", "e", "0", "f", "0", "g")
	c.expect(strs("a", "b", "c"), "ZRANGEBYLEX", "lex", "-", "[c")
	c.expect(strs("a", "b"), "ZRANGEBYLEX", "lex", "-", "(c")
	c.expect(strs("b", "c", "d", "e", "f"), "ZRANGEBYLEX", "lex", "[aaa", "(g")
	c.expect(strs("c", "b", "a"), "ZREVRANGEBYLEX", "lex", "[c", "-")
	c.expect(strs("d", "c", "b"), "ZRANGE", "lex", "(e", "[b", "BYLEX", "REV")
	c.expect(strs("c", "d"), "ZRANGEBYLEX", "lex", "-", "+", "LIMIT", "2", "2")
	c.expect(int64(7), "ZLEXCOUNT", "lex", "-", "+")
	c.expect(int64(5), "ZLEXCOUNT", "lex", "[b", "[f")
	c.expect(int64(0), "ZLEXCOUNT", "lex", "+", "-")
	c.expect(errReply("ERR min or max not valid string range item"), "ZRANGEBYLEX", "lex", "a", "b")
	c.expect(int64(5), "ZADD", "rl", "0", "aaaa", "0", "b", "0", "c", "0", "d", "0", "e")
	c.expect(int64(5), "ZADD", "rl", "0", "foo", "0", "zap", "0", "zip", "0", "ALPHA", "0", "alpha")
	c.expect(int64(6), "ZREMRANGEBYLEX", "rl", "[alpha", "[omega")
	c.expect(strs("ALPHA", "aaaa", "zap", "zip"), "ZRANGE", "rl", "0", "-1")

	c.expect(int64(3), "ZADD", "r", "1", "one", "2", "two", "3", "three")
	c.expect(int64(2), "ZREMRANGEBYRANK", "r", "0", "1")
	c.expect(strs("three", "3"), "ZRANGE", "r", "0", "-1", "WITHSCORES")
	c.expect(int64(2), "ZADD", "r", "1", "one", "2", "two")
	c.expect(int64(1), "ZREMRANGEBYSCORE", "r", "-inf", "(2")
	c.expect(strs("two", "2", "three", "3"), "ZRANGE", "r", "0", "-1", "WITHSCORES")
	c.expect(int64(2), "ZREM", "r", "two", "three", "nope")
	c.expect(int64(0), "EXISTS", "r")

	c.expect(int64(3), "ZADD", "p", "1", "one", "2", "two", "3", "three")
	c.expect(strs("one", "1"), "ZPOPMIN", "p")
	c.expect(strs("three", "3", "two", "2"), "ZPOPMAX", "p", "2")
	c.expect(strs(), "ZPOPMIN", "p")
	c.expect(int64(0), "EXISTS", "p")
	c.expect(errReply("ERR value is out of range, must be positive"), "ZPOPMIN", "p", "-1")
	c.expect(nil, "ZMPOP", "1", "p", "MIN")
	c.expect(int64(3), "ZADD", "p", "1", "one", "2", "two", "3", "three")
	c.expect([]any{"p", []any{strs("one", "1")}}, "ZMPOP", "1", "p", "MIN")
	c.expect([]any{"p", []any{strs("three", "3"), strs("two", "2")}}, "ZMPOP", "2", "none", "p", "MAX", "COUNT", "10")
	c.expect(errReply("ERR numkeys should be greater than 0"), "ZMPOP", "0", "p", "MIN")
	c.expect(errReply(errSyntax), "ZMPOP", "1", "p", "LEFT")
	c.expect(errReply(errSyntax), "ZMPOP", "3", "p", "MIN")
	c.expect(errReply("ERR count should be greater than 0"), "ZMPOP", "1", "p", "MIN", "COUNT", "0")

	c.expect(int64(2), "ZADD", "zset1", "1", "one", "2", "two")
	c.expect(int64(3), "ZADD", "zset2", "1", "one", "2", "two", "3", "three")
	c.expect(int64(3), "ZUNIONSTORE", "out", "2", "zset1", "zset2", "WEIGHTS", "2", "3")
	c.expect(strs("one", "5", "three", "9", "two", "10"), "ZRANGE", "out", "0", "-1", "WITHSCORES")
	c.expect(int64(2), "ZINTERSTORE", "out", "2", "zset1", "zset2", "WEIGHTS", "2", "3")
	c.expect(strs("one", "5", "two", "10"), "ZRANGE", "out", "0", "-1", "WITHSCORES")
	c.expect(strs("one", "three", "two"), "ZUNION", "2", "zset1", "zset2")
	c.expect(strs("one", "2", "three", "3", "two", "4"), "ZUNION", "2", "zset1", "zset2", "WITHSCORES")
	c.expect(strs("one", "1", "two", "2", "three", "3"), "ZUNION", "2", "zset1", "zset2", "AGGREGATE", "MAX", "WITHSCORES")
	c.expect(strs("one", "2", "two", "4"), "ZINTER", "2", "zset1", "zset2", "WITHSCORES")
	c.expect(strs("three", "3"), "ZDIFF", "2", "zset2", "zset1", "WITHSCORES")
	c.expect(int64(1), "ZDIFFSTORE", "out", "2", "zset2", "zset1")
	c.expect(strs("three"), "ZRANGE", "out", "0", "-1")
	c.expect(int64(0), "ZDIFFSTORE", "out", "2", "zset1", "zset2")
	c.expect(int64(0), "EXISTS", "out")
	c.expect(int64(2), "ZINTERCARD", "2", "zset1", "zset2")
	c.expect(int64(1), "ZINTERCARD", "2", "zset1", "zset2", "LIMIT", "1")
	c.expect(errReply("ERR at least 1 input key is needed for 'zintercard' command"), "ZINTERCARD", "0", "zset1")
	c.expect(errReply("ERR LIMIT can't be negative"), "ZINTERCARD", "1", "zset1", "LIMIT", "-1")
	c.expect(errReply(errSyntax), "ZUNION", "3", "zset1", "zset2")
	c.expect(errReply(errSyntax), "ZDIFF", "2", "zset1", "zset2", "WEIGHTS", "1", "1")
	c.expect(errReply("ERR weight value is not a float"), "ZUNION", "2", "zset1", "zset2", "WEIGHTS", "1", "x")
	c.expect(int64(1), "SADD", "plain", "one")
	c.expect(strs("one", "2", "two", "2"), "ZUNION", "2", "zset1", "plain", "WITHSCORES")
	c.expect(int64(2), "ZRANGESTORE", "dst", "zset2", "0", "1")
	c.expect(strs("one", "two"), "ZRANGE", "dst", "0", "-1")
	c.expect(int64(2), "ZRANGESTORE", "dst", "zset2", "(1", "+inf", "BYSCORE")
	c.expect(strs("two", "three"), "ZRANGE", "dst", "0", "-1")
	c.expect(errReply(errSyntax), "ZRANGESTORE", "dst", "zset2", "0", "1", "WITHSCORES")

	c.expect(int64(1), "ZADD", "f", "1", "a")
	c.expect(int64(1), "ZADD", "f", "NX", "2", "a", "3", "b")
	c.expect("1", "ZSCORE", "f", "a")
	c.expect(int64(0), "ZADD", "f", "XX", "5", "a", "7", "c")
	c.expect("5", "ZSCORE", "f", "a")
	c.expect(nil, "ZSCORE", "f", "c")
	c.expect(int64(1), "ZADD", "f", "XX", "CH", "6", "a")
	c.expect(int64(0), "ZADD", "f", "GT", "CH", "4", "a")
	c.expect(int64(1), "ZADD", "f", "GT", "CH", "8", "a")
	c.expect(int64(0), "ZADD", "f", "LT", "CH", "9", "a")
	c.expect(int64(0), "ZADD", "f", "CH", "8", "a")
	c.expect("10", "ZADD", "f", "INCR", "2", "a")
	c.expect(nil, "ZADD", "f", "NX", "INCR", "1", "a")
	c.expect(nil, "ZADD", "missing", "XX", "INCR", "1", "a")
	c.expect(int64(0), "EXISTS", "missing")
	c.expect(errReply("ERR XX and NX options"), "ZADD", "f", "XX", "NX", "1", "a")
	c.expect(errReply("ERR GT, LT, and/or NX options"), "ZADD", "f", "GT", "LT", "1", "a")
	c.expect(errReply("ERR INCR option supports a single increment-element pair"), "ZADD", "f", "INCR", "1", "a", "2", "b")
	c.expect(errReply(errSyntax), "ZADD", "f", "NX", "1")
	c.expect(errReply(errNotFloat), "ZADD", "f", "nan", "a")
	c.expect(int64(1), "ZADD", "f", "+inf", "i")
	c.expect("inf", "ZSCORE", "f", "i")
	c.expect(errReply("ERR resulting score is not a number (NaN)"), "ZINCRBY", "f", "-inf", "i")
	c.expect(int64(6), "ZADD", "fmt", "1.5", "a", "0.1", "b", "1e20", "c", "1e-5", "d", "100000000", "e", "1.1", "g")
	c.expect([]any{"1.5", "0.1", "1e+20", "1e-05", "100000000"}, "ZMSCORE", "fmt", "a", "b", "c", "d", "e")
	c.expect("3.3000000000000003", "ZINCRBY", "fmt", "2.2", "g")
	c.expect(int64(1), "ZADD", "fmt", "-0", "z")
	c.expect("0", "ZSCORE", "fmt", "z")

	if m, _ := c.do("ZRANDMEMBER", "myzset").(string); !slices.Contains([]string{"one", "uno", "two", "three"}, m) {
		t.Fatalf("ZRANDMEMBER = %q", m)
	}
	if got, _ := c.do("ZRANDMEMBER", "myzset", "-5", "WITHSCORES").([]any); len(got) != 10 {
		t.Fatalf("ZRANDMEMBER myzset -5 WITHSCORES = %v", got)
	}
	if got := members(t, c, "ZRANDMEMBER", "myzset", "10"); !slices.Equal(got, []string{"one", "three", "two", "uno"}) {
		t.Fatalf("ZRANDMEMBER myzset 10 = %v", got)
	}
	c.expect([]any{"0", strs("two", "2", "three", "3")}, "ZSCAN", "myzset", "0", "MATCH", "t*")

	c.expect(status("OK"), "SET", "str", "v")
	c.expect(errReply(errWrongType), "ZADD", "str", "1", "a")
	c.expect(errReply(errWrongType), "ZRANGE", "str", "0", "-1")
	c.expect(errReply(errWrongType), "ZUNION", "1", "str")
	c.expect(errReply(errNotFloat), "ZADD", "str", "x", "a")
}

func TestSortedSetTable(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	c := dial(t, addr)
	args := []string{"ZADD", "big"}
	for i := range 200 {
		args = append(args, fmt.Sprint(i%50), fmt.Sprintf("m%03d", i))
	}
	c.expect(int64(200), args...)
	c.expect("skiplist", "OBJECT", "ENCODING", "big")
	c.expect(status("zset"), "TYPE", "big")
	c.expect(int64(200), "ZCARD", "big")
	c.expect(strs("m000", "0", "m050", "0"), "ZRANGE", "big", "0", "1", "WITHSCORES")
	c.expect(int64(18), "ZRANK", "big", "m104")
	c.expect(int64(8), "ZCOUNT", "big", "(1", "3")
	c.expect(strs("m199", "m149"), "ZREVRANGE", "big", "0", "1")
	c.expect(strs("m002", "m052"), "ZRANGEBYSCORE", "big", "2", "+inf", "LIMIT", "0", "2")
	c.expect("100", "ZINCRBY", "big", "100", "m000")
	c.expect(strs("m000", "100"), "ZPOPMAX", "big")
	c.expect(int64(3), "ZREMRANGEBYSCORE", "big", "0", "0")
	c.expect(int64(196), "ZCARD", "big")
	c.expect(int64(1), "ZADD", "long", "1", strings.Repeat("x", 65))
	c.expect("skiplist", "OBJECT", "ENCODING", "long")
	c.expect(int64(1), "EXPIRE", "big", "100")
	c.expect(int64(1), "ZADD", "big", "-1", "new")
	c.expect(int64(100), "TTL", "big")
	c.expect(status("OK"), "MULTI")
	c.expect(status("QUEUED"), "ZADD", "big", "-2", "first")
	c.expect(status("QUEUED"), "ZRANGE", "big", "0", "1")
	c.expect(status("QUEUED"), "ZRANK", "big", "new")
	c.expect([]any{int64(1), strs("first", "new"), int64(1)}, "EXEC")
	c.expect(status("OK"), "MULTI")
	c.expect(status("QUEUED"), "DEL", "long")
	c.expect(status("QUEUED"), "ZADD", "long", "1", "x")
	c.expect([]any{int64(1), int64(1)}, "EXEC")
	c.expect("listpack", "OBJECT", "ENCODING", "long")

	check := func() {
		t.Helper()
		c := dial(t, addr)
		c.expect(int64(198), "ZCARD", "big")
		c.expect(strs("first", "-2", "new", "-1", "m001", "1"), "ZRANGE", "big", "0", "2", "WITHSCORES")
		c.expect(strs("m199", "49"), "ZRANGE", "big", "-1", "-1", "WITHSCORES")
		c.expect(int64(16), "ZRANK", "big", "m104")
		c.expect(strs("x", "1"), "ZRANGE", "long", "0", "-1", "WITHSCORES")
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

type zmember struct {
	member string
	score  float64
}

func TestSortedSetModel(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	model := map[string]float64{}
	sorted := func() []zmember {
		var out []zmember
		for m, s := range model {
			out = append(out, zmember{m, s})
		}
		slices.SortFunc(out, func(a, b zmember) int {
			return cmp.Or(cmp.Compare(a.score, b.score), strings.Compare(a.member, b.member))
		})
		return out
	}
	flat := func(items []zmember, withScores bool) []any {
		out := []any{}
		for _, it := range items {
			out = append(out, it.member)
			if withScores {
				out = append(out, formatScore(it.score))
			}
		}
		return out
	}
	rng := rand.New(rand.NewPCG(7, 8))
	score := func() float64 { return float64(rng.IntN(41)-20) / 4 }
	tables := 0
	for range 5000 {
		m := fmt.Sprint(rng.IntN(220))
		switch op := rng.IntN(14); {
		case op < 5:
			s := score()
			_, had := model[m]
			model[m] = s
			c.expect(int64(map[bool]int{true: 0, false: 1}[had]), "ZADD", "z", formatScore(s), m)
		case op == 5:
			_, had := model[m]
			delete(model, m)
			c.expect(int64(map[bool]int{true: 1, false: 0}[had]), "ZREM", "z", m)
		case op == 6:
			s := score()
			model[m] += s
			c.expect(formatScore(model[m]), "ZINCRBY", "z", formatScore(s), m)
		case op == 7 && len(model) > 0:
			n := rng.IntN(3) + 1
			items := sorted()
			highest := rng.IntN(2) == 0
			if highest {
				slices.Reverse(items)
			}
			items = items[:min(n, len(items))]
			for _, it := range items {
				delete(model, it.member)
			}
			c.expect(flat(items, true), map[bool]string{false: "ZPOPMIN", true: "ZPOPMAX"}[highest], "z", fmt.Sprint(n))
		case op == 8:
			items := sorted()
			if _, ok := model[m]; ok {
				rank := slices.IndexFunc(items, func(it zmember) bool { return it.member == m })
				c.expect(int64(rank), "ZRANK", "z", m)
				c.expect(int64(len(items)-1-rank), "ZREVRANK", "z", m)
			} else {
				c.expect(nil, "ZRANK", "z", m)
			}
		case op == 9:
			lo, hi := score(), score()
			var in []zmember
			for _, it := range sorted() {
				if lo < it.score && it.score <= hi {
					in = append(in, it)
				}
			}
			c.expect(int64(len(in)), "ZCOUNT", "z", "("+formatScore(lo), formatScore(hi))
			offset, count := rng.IntN(5), rng.IntN(6)-1
			want := in[min(offset, len(in)):]
			if count >= 0 {
				want = want[:min(count, len(want))]
			}
			c.expect(flat(want, true), "ZRANGEBYSCORE", "z", "("+formatScore(lo), formatScore(hi), "WITHSCORES", "LIMIT", fmt.Sprint(offset), fmt.Sprint(count))
			slices.Reverse(in)
			want = in[min(offset, len(in)):]
			if count >= 0 {
				want = want[:min(count, len(want))]
			}
			c.expect(flat(want, false), "ZRANGE", "z", formatScore(hi), "("+formatScore(lo), "BYSCORE", "REV", "LIMIT", fmt.Sprint(offset), fmt.Sprint(count))
		case op == 10 && len(model) > 150:
			items := sorted()
			for _, it := range items[10:20] {
				delete(model, it.member)
			}
			c.expect(int64(10), "ZREMRANGEBYRANK", "z", "10", "19")
		default:
			items := sorted()
			c.expect(flat(items, true), "ZRANGE", "z", "0", "-1", "WITHSCORES")
			start, stop := rng.IntN(20)-10, rng.IntN(20)-10
			slices.Reverse(items)
			from, to := start, stop
			if from < 0 {
				from += len(items)
			}
			if to < 0 {
				to += len(items)
			}
			from, to = max(from, 0), min(to, len(items)-1)
			want := []zmember{}
			if from <= to {
				want = items[from : to+1]
			}
			c.expect(flat(want, false), "ZREVRANGE", "z", fmt.Sprint(start), fmt.Sprint(stop))
			c.expect(int64(len(model)), "ZCARD", "z")
			if enc, _ := c.do("OBJECT", "ENCODING", "z").(string); enc == "skiplist" {
				tables++
			}
		}
	}
	if tables == 0 {
		t.Fatal("the sorted set never became a skiplist")
	}
	c.expect(flat(sorted(), true), "ZRANGE", "z", "0", "-1", "WITHSCORES")
}
