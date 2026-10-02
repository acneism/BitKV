package server

import (
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

func strs(values ...string) []any {
	out := make([]any, len(values))
	for i, v := range values {
		out[i] = v
	}
	return out
}

func TestLists(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	c := dial(t, addr)
	c.expect(int64(1), "LPUSH", "l", "world")
	c.expect(int64(2), "LPUSH", "l", "hello")
	c.expect(strs("hello", "world"), "LRANGE", "l", "0", "-1")
	c.expect(int64(3), "LPUSHX", "l", "x")
	c.expect(int64(0), "LPUSHX", "missing", "x")
	c.expect(int64(0), "RPUSHX", "missing", "x")
	c.expect(status("list"), "TYPE", "l")
	c.expect("listpack", "OBJECT", "ENCODING", "l")

	c.expect(int64(5), "RPUSH", "p", "one", "two", "three", "four", "five")
	c.expect("one", "LPOP", "p")
	c.expect(strs("two", "three"), "LPOP", "p", "2")
	c.expect("five", "RPOP", "p")
	c.expect(strs("four"), "LRANGE", "p", "0", "-1")
	c.expect([]any{}, "LPOP", "p", "0")
	c.expect(nil, "LPOP", "missing")
	c.expect(nil, "LPOP", "missing", "2")
	c.expect(errReply("ERR value is out of range, must be positive"), "LPOP", "p", "-1")
	c.expect(strs("four"), "RPOP", "p", "10")
	c.expect(int64(0), "EXISTS", "p")

	c.expect(int64(3), "RPUSH", "r", "one", "two", "three")
	c.expect("one", "LINDEX", "r", "0")
	c.expect("three", "LINDEX", "r", "-1")
	c.expect(nil, "LINDEX", "r", "3")
	for want, rng := range map[string][2]string{"one": {"0", "0"}, "one two three": {"-3", "2"}, "one two three ": {"-100", "100"}, "": {"5", "10"}, " ": {"0", "-100"}} {
		got, _ := c.do("LRANGE", "r", rng[0], rng[1]).([]any)
		joined := ""
		for i, v := range got {
			if i > 0 {
				joined += " "
			}
			joined += v.(string)
		}
		if joined != strings.TrimSpace(want) {
			t.Fatalf("LRANGE r %v = %q, want %q", rng, joined, want)
		}
	}
	c.expect(status("OK"), "LSET", "r", "0", "four")
	c.expect(status("OK"), "LSET", "r", "-2", "five")
	c.expect(strs("four", "five", "three"), "LRANGE", "r", "0", "-1")
	c.expect(errReply("ERR index out of range"), "LSET", "r", "3", "x")
	c.expect(errReply("ERR no such key"), "LSET", "missing", "0", "x")

	c.expect(int64(4), "RPUSH", "rem", "hello", "hello", "foo", "hello")
	c.expect(int64(2), "LREM", "rem", "-2", "hello")
	c.expect(strs("hello", "foo"), "LRANGE", "rem", "0", "-1")
	c.expect(int64(1), "LREM", "rem", "0", "hello")
	c.expect(status("OK"), "LTRIM", "r", "1", "-1")
	c.expect(strs("five", "three"), "LRANGE", "r", "0", "-1")
	c.expect(status("OK"), "LTRIM", "r", "5", "10")
	c.expect(int64(0), "EXISTS", "r")

	c.expect(int64(2), "RPUSH", "ins", "Hello", "World")
	c.expect(int64(3), "LINSERT", "ins", "BEFORE", "World", "There")
	c.expect(strs("Hello", "There", "World"), "LRANGE", "ins", "0", "-1")
	c.expect(int64(-1), "LINSERT", "ins", "AFTER", "nope", "x")
	c.expect(int64(0), "LINSERT", "missing", "AFTER", "nope", "x")

	c.expect(int64(11), "RPUSH", "pos", "a", "b", "c", "d", "1", "2", "3", "4", "3", "3", "3")
	c.expect(int64(6), "LPOS", "pos", "3")
	c.expect([]any{int64(8), int64(9), int64(10)}, "LPOS", "pos", "3", "COUNT", "0", "RANK", "2")
	c.expect(int64(10), "LPOS", "pos", "3", "RANK", "-1")
	c.expect(nil, "LPOS", "pos", "3", "MAXLEN", "5")
	c.expect([]any{}, "LPOS", "pos", "nope", "COUNT", "1")
	c.expect(errReply("ERR RANK can't be zero"), "LPOS", "pos", "3", "RANK", "0")

	c.expect(int64(3), "RPUSH", "mv", "one", "two", "three")
	c.expect("three", "LMOVE", "mv", "other", "RIGHT", "LEFT")
	c.expect("one", "LMOVE", "mv", "other", "LEFT", "RIGHT")
	c.expect(strs("two"), "LRANGE", "mv", "0", "-1")
	c.expect(strs("three", "one"), "LRANGE", "other", "0", "-1")
	c.expect("one", "RPOPLPUSH", "other", "other")
	c.expect(strs("one", "three"), "LRANGE", "other", "0", "-1")
	c.expect(nil, "LMOVE", "missing", "other", "LEFT", "LEFT")
	c.expect(status("OK"), "SET", "str", "v")
	c.expect(errReply(errWrongType), "LPUSH", "str", "x")
	c.expect(errReply(errWrongType), "LMOVE", "mv", "str", "LEFT", "LEFT")

	args := []string{"RPUSH", "big"}
	for i := range 200 {
		args = append(args, fmt.Sprint(i))
	}
	c.expect(int64(200), args...)
	c.expect("quicklist", "OBJECT", "ENCODING", "big")
	c.expect(int64(201), "LPUSH", "big", "head")
	c.expect("head", "LINDEX", "big", "0")
	c.expect("199", "LINDEX", "big", "-1")
	c.expect(strs("head", "0", "1"), "LRANGE", "big", "0", "2")
	c.expect(int64(1), "EXPIRE", "big", "100")
	c.expect("199", "RPOP", "big")
	c.expect(int64(100), "TTL", "big")
	c.expect(status("OK"), "LTRIM", "big", "1", "-1")
	c.expect(int64(199), "LLEN", "big")
	stopServer(t, srv, db)

	srv, db, addr = startServer(t, dir)
	defer stopServer(t, srv, db)
	c = dial(t, addr)
	c.expect(int64(199), "LLEN", "big")
	c.expect(strs("0", "1"), "LRANGE", "big", "0", "1")
	c.expect("198", "LINDEX", "big", "-1")
	c.expect(strs("Hello", "There", "World"), "LRANGE", "ins", "0", "-1")
}

func TestListModel(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	var model []string
	tables := 0
	rng := rand.New(rand.NewPCG(5, 6))
	value := func() string { return fmt.Sprint(rng.IntN(20)) }
	for i := range 4000 {
		switch op := rng.IntN(12); {
		case op < 3:
			v := value()
			model = append([]string{v}, model...)
			c.expect(int64(len(model)), "LPUSH", "m", v)
		case op < 6:
			v := value()
			model = append(model, v)
			c.expect(int64(len(model)), "RPUSH", "m", v)
		case op == 6 && len(model) > 0:
			c.expect(model[0], "LPOP", "m")
			model = model[1:]
		case op == 7 && len(model) > 0:
			c.expect(model[len(model)-1], "RPOP", "m")
			model = model[:len(model)-1]
		case op == 8 && len(model) > 0:
			j, v := rng.IntN(len(model)), value()
			model = slices.Clone(model)
			model[j] = v
			c.expect(status("OK"), "LSET", "m", fmt.Sprint(j), v)
		case op == 9 && len(model) > 0:
			pivot, v := model[rng.IntN(len(model))], value()
			j := slices.Index(model, pivot)
			model = slices.Insert(slices.Clone(model), j, v)
			c.expect(int64(len(model)), "LINSERT", "m", "BEFORE", pivot, v)
		case op == 10:
			v := value()
			kept := slices.DeleteFunc(slices.Clone(model), func(x string) bool { return x == v })
			c.expect(int64(len(model)-len(kept)), "LREM", "m", "0", v)
			model = kept
		case op == 11 && len(model) > 160:
			c.expect(status("OK"), "LTRIM", "m", "10", "-11")
			model = slices.Clone(model[10 : len(model)-10])
		default:
			got, _ := c.do("LRANGE", "m", "0", "-1").([]any)
			if !slices.Equal(got, strs(model...)) {
				t.Fatalf("step %d: LRANGE = %v, model %v", i, got, model)
			}
			if enc, _ := c.do("OBJECT", "ENCODING", "m").(string); enc == "quicklist" {
				tables++
			}
		}
	}
	got, _ := c.do("LRANGE", "m", "0", "-1").([]any)
	if !slices.Equal(got, strs(model...)) || tables == 0 {
		t.Fatalf("LRANGE = %v, model %v, checked as quicklist %d times", got, model, tables)
	}
}
