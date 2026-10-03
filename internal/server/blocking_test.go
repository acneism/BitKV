package server

import (
	"fmt"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func (c *testConn) send(args ...string) {
	c.t.Helper()
	if _, err := io.WriteString(c.conn, encode(args...)); err != nil {
		c.t.Fatal(err)
	}
}

func (c *testConn) expectRead(want any) {
	c.t.Helper()
	if got := c.read(); !reflect.DeepEqual(got, want) {
		c.t.Fatalf("got %#v, want %#v", got, want)
	}
}

func waitBlocked(t *testing.T, c *testConn, n int) {
	t.Helper()
	want := fmt.Sprintf("blocked_clients:%d\r\n", n)
	for range 500 {
		if info, _ := c.do("INFO").(string); strings.Contains(info, want) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("blocked_clients never became %d", n)
}

func TestBlockingCommands(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	a, b, c := dial(t, addr), dial(t, addr), dial(t, addr)

	c.expect(int64(2), "RPUSH", "q", "x", "y")
	c.expect(strs("q", "x"), "BLPOP", "none", "q", "0")
	c.expect(strs("q", "y"), "BRPOP", "q", "0")
	start := time.Now()
	c.expect(nil, "BLPOP", "q", "0.05")
	if d := time.Since(start); d < 50*time.Millisecond {
		t.Fatalf("BLPOP with a 50 ms timeout returned after %v", d)
	}

	a.send("BLPOP", "q", "0")
	waitBlocked(t, c, 1)
	b.send("BRPOP", "q", "0")
	waitBlocked(t, c, 2)
	c.expect(int64(1), "RPUSH", "q", "first")
	a.expectRead(strs("q", "first"))
	waitBlocked(t, c, 1)
	c.expect(int64(1), "RPUSH", "q", "second")
	b.expectRead(strs("q", "second"))

	a.send("BLMOVE", "src", "dst", "RIGHT", "LEFT", "0")
	waitBlocked(t, c, 1)
	c.expect(int64(1), "LPUSH", "src", "v")
	a.expectRead("v")
	c.expect(strs("v"), "LRANGE", "dst", "0", "-1")
	a.send("BRPOPLPUSH", "src", "dst", "0")
	b.send("BLPOP", "dst2", "0")
	waitBlocked(t, c, 2)
	c.expect(int64(1), "RPUSH", "src", "w")
	a.expectRead("w")
	c.expect("w", "LMOVE", "dst", "dst2", "LEFT", "LEFT")
	b.expectRead(strs("dst2", "w"))

	c.expect(nil, "LMPOP", "1", "none", "LEFT")
	c.expect(int64(3), "RPUSH", "m", "1", "2", "3")
	c.expect([]any{"m", strs("1", "2")}, "LMPOP", "2", "none", "m", "LEFT", "COUNT", "2")
	c.expect([]any{"m", strs("3")}, "BLMPOP", "0", "1", "m", "RIGHT", "COUNT", "5")
	c.expect(errReply("ERR numkeys should be greater than 0"), "LMPOP", "0", "m", "LEFT")
	c.expect(errReply("ERR count should be greater than 0"), "BLMPOP", "0", "1", "m", "LEFT", "COUNT", "0")
	c.expect(errReply(errSyntax), "LMPOP", "1", "m", "UP")
	a.send("BLMPOP", "0", "2", "m", "n", "LEFT", "COUNT", "2")
	waitBlocked(t, c, 1)
	c.expect(int64(3), "RPUSH", "n", "a", "b", "c")
	a.expectRead([]any{"n", strs("a", "b")})

	a.send("BZPOPMIN", "z1", "z2", "0")
	waitBlocked(t, c, 1)
	c.expect(int64(2), "ZADD", "z2", "2", "b", "1", "a")
	a.expectRead(strs("z2", "a", "1"))
	c.expect(strs("z2", "b", "2"), "BZPOPMAX", "z2", "0")
	c.expect(nil, "BZPOPMAX", "z2", "0.01")
	a.send("BZMPOP", "0", "1", "z3", "MAX", "COUNT", "2")
	waitBlocked(t, c, 1)
	c.expect(int64(3), "ZADD", "z3", "1", "a", "2", "b", "3", "c")
	a.expectRead([]any{"z3", []any{strs("c", "3"), strs("b", "2")}})

	c.expect(status("OK"), "MULTI")
	c.expect(status("QUEUED"), "BLPOP", "none", "0")
	c.expect(status("QUEUED"), "BLMOVE", "none", "dst", "LEFT", "LEFT", "0")
	c.expect(status("QUEUED"), "BZPOPMIN", "none", "0")
	c.expect([]any{nil, nil, nil}, "EXEC")

	c.expect(errReply("ERR timeout is negative"), "BLPOP", "q", "-1")
	c.expect(errReply("ERR timeout is not a float or out of range"), "BLPOP", "q", "x")
	c.expect(errReply("ERR timeout is out of range"), "BLPOP", "q", "inf")
	c.expect(status("OK"), "SET", "str", "v")
	c.expect(errReply(errWrongType), "BLPOP", "str", "0")
	c.expect(errReply(errWrongType), "BZPOPMIN", "str", "0")

	a.send("BLPOP", "p", "0")
	a.send("PING")
	waitBlocked(t, c, 1)
	c.expect(int64(1), "RPUSH", "p", "x")
	a.expectRead(strs("p", "x"))
	a.expectRead(status("PONG"))

	d := dial(t, addr)
	d.send("BLPOP", "gone", "0")
	waitBlocked(t, c, 1)
	d.conn.Close()
	waitBlocked(t, c, 0)
	c.expect(int64(1), "RPUSH", "gone", "kept")
	c.expect(int64(1), "LLEN", "gone")
}

func TestCloseReleasesBlockedClients(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	a, c := dial(t, addr), dial(t, addr)
	a.send("BLPOP", "q", "0")
	waitBlocked(t, c, 1)
	stopServer(t, srv, db)
}

func TestBlockedClientIgnoresIdleTimeout(t *testing.T) {
	srv, db, addr := startServerWith(t, t.TempDir(), Config{Timeout: 100 * time.Millisecond})
	defer stopServer(t, srv, db)
	a := dial(t, addr)
	a.send("BLPOP", "q", "0")
	waitBlocked(t, dial(t, addr), 1)
	time.Sleep(300 * time.Millisecond)
	dial(t, addr).expect(int64(1), "RPUSH", "q", "x")
	a.expectRead(strs("q", "x"))
	a.expect(status("PONG"), "PING")
}
