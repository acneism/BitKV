package server

import (
	"fmt"
	"maps"
	"math/rand/v2"
	"slices"
	"testing"
)

func TestHashes(t *testing.T) {
	dir := t.TempDir()
	srv, db, addr := startServer(t, dir)
	c := dial(t, addr)
	c.expect(int64(1), "HSET", "h", "field1", "Hello")
	c.expect("Hello", "HGET", "h", "field1")
	c.expect(int64(2), "HSET", "h", "field2", "Hi", "field3", "World")
	c.expect(int64(0), "HSET", "h", "field2", "Hey")
	c.expect([]any{"field1", "Hello", "field2", "Hey", "field3", "World"}, "HGETALL", "h")
	c.expect([]any{"field1", "field2", "field3"}, "HKEYS", "h")
	c.expect([]any{"Hello", "Hey", "World"}, "HVALS", "h")
	c.expect([]any{"Hello", nil, "World"}, "HMGET", "h", "field1", "nofield", "field3")
	c.expect(int64(3), "HLEN", "h")
	c.expect(int64(1), "HEXISTS", "h", "field1")
	c.expect(int64(0), "HEXISTS", "h", "nofield")
	c.expect(int64(5), "HSTRLEN", "h", "field1")
	c.expect(int64(0), "HSTRLEN", "h", "nofield")
	c.expect(int64(1), "HSETNX", "h", "field4", "x")
	c.expect(int64(0), "HSETNX", "h", "field4", "y")
	c.expect(status("OK"), "HMSET", "h", "field5", "a", "field6", "b")
	c.expect(errReply("ERR wrong number of arguments for 'hset' command"), "HSET", "h", "f", "v", "g")
	c.expect(int64(2), "HDEL", "h", "field4", "field5", "nofield")
	c.expect(status("hash"), "TYPE", "h")
	c.expect("listpack", "OBJECT", "ENCODING", "h")
	c.expect([]any{"0", []any{"h"}}, "SCAN", "0", "TYPE", "hash")
	c.expect(errReply(errWrongType), "GET", "h")
	c.expect(status("OK"), "SET", "s", "v")
	c.expect(errReply(errWrongType), "HSET", "s", "f", "v")
	c.expect(errReply(errWrongType), "HGETALL", "s")

	c.expect(int64(1), "HSET", "n", "i", "5")
	c.expect(int64(6), "HINCRBY", "n", "i", "1")
	c.expect(int64(-4), "HINCRBY", "n", "i", "-10")
	c.expect(int64(10), "HINCRBY", "n", "new", "10")
	c.expect(errReply("ERR hash value is not an integer"), "HINCRBY", "h", "field1", "1")
	c.expect(status("OK"), "HMSET", "n", "big", "9223372036854775807", "f", "10.50")
	c.expect(errReply(errOverflow), "HINCRBY", "n", "big", "1")
	c.expect("10.6", "HINCRBYFLOAT", "n", "f", "0.1")
	c.expect("5.6", "HINCRBYFLOAT", "n", "f", "-5")
	c.expect(errReply("ERR hash value is not a float"), "HINCRBYFLOAT", "h", "field1", "1")
	c.expect(errReply("ERR value is NaN or Infinity"), "HINCRBYFLOAT", "n", "f", "inf")

	c.expect([]any{"0", []any{"field1", "Hello", "field3", "World"}}, "HSCAN", "h", "0", "MATCH", "field[13]")
	c.expect([]any{"0", []any{"field1", "field2"}}, "HSCAN", "h", "0", "MATCH", "field[12]", "NOVALUES")
	c.expect(errReply("ERR invalid cursor"), "HSCAN", "h", "x")

	c.expect(int64(3), "HSET", "coin", "heads", "obverse", "tails", "reverse", "edge", "null")
	if field, _ := c.do("HRANDFIELD", "coin").(string); !slices.Contains([]string{"heads", "tails", "edge"}, field) {
		t.Fatalf("HRANDFIELD coin = %q", field)
	}
	if got, _ := c.do("HRANDFIELD", "coin", "-5", "WITHVALUES").([]any); len(got) != 10 {
		t.Fatalf("HRANDFIELD coin -5 WITHVALUES = %#v", got)
	}
	if got, _ := c.do("HRANDFIELD", "coin", "10").([]any); len(got) != 3 {
		t.Fatalf("HRANDFIELD coin 10 = %#v", got)
	}
	c.expect(nil, "HRANDFIELD", "missing")
	c.expect([]any{}, "HRANDFIELD", "missing", "3")

	c.expect(int64(1), "EXPIRE", "coin", "100")
	c.expect(int64(1), "HSET", "coin", "side", "x")
	c.expect(int64(100), "TTL", "coin")
	c.expect(int64(4), "HDEL", "coin", "heads", "tails", "edge", "side")
	c.expect(int64(0), "EXISTS", "coin")
	stopServer(t, srv, db)

	srv, db, addr = startServer(t, dir)
	defer stopServer(t, srv, db)
	dial(t, addr).expect([]any{"field1", "Hello", "field2", "Hey", "field3", "World", "field6", "b"}, "HGETALL", "h")
}

func TestHashModel(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	model := map[string]string{}
	rng := rand.New(rand.NewPCG(1, 2))
	for i := range 2000 {
		field := fmt.Sprintf("f%d", rng.IntN(8))
		switch rng.IntN(4) {
		case 0, 1:
			value := fmt.Sprint(rng.IntN(1000))
			_, existed := model[field]
			model[field] = value
			c.expect(int64(map[bool]int{true: 0, false: 1}[existed]), "HSET", "m", field, value)
		case 2:
			_, existed := model[field]
			delete(model, field)
			c.expect(int64(map[bool]int{true: 1, false: 0}[existed]), "HDEL", "m", field)
		default:
			got, _ := c.do("HGETALL", "m").([]any)
			pairs := map[string]string{}
			for j := 0; j+1 < len(got); j += 2 {
				pairs[got[j].(string)] = got[j+1].(string)
			}
			if !maps.Equal(pairs, model) {
				t.Fatalf("step %d: HGETALL = %v, model %v", i, pairs, model)
			}
			c.expect(int64(len(model)), "HLEN", "m")
		}
	}
}
