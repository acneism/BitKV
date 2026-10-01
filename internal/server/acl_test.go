package server

import (
	"crypto/sha256"
	"encoding/hex"
	"io"
	"slices"
	"strings"
	"testing"
)

func TestACLLog(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	admin := dial(t, addr)
	admin.expect(status("OK"), "ACL", "SETUSER", "bob", "on", ">pw", "~b:*", "+get", "+multi", "+discard")
	bob := dial(t, addr)
	bob.expect(errReply("WRONGPASS"), "AUTH", "bob", "wrong")
	bob.expect(status("OK"), "AUTH", "bob", "pw")
	bob.expect(errReply("NOPERM"), "GET", "a:1")
	bob.expect(errReply("NOPERM"), "GET", "a:1")
	bob.expect(status("OK"), "MULTI")
	bob.expect(errReply("NOPERM"), "SET", "b:1", "v")
	bob.expect(status("OK"), "DISCARD")

	log, _ := admin.do("ACL", "LOG").([]any)
	want := [][]any{
		{int64(1), "command", "multi", "set", "bob"},
		{int64(2), "key", "toplevel", "a:1", "bob"},
		{int64(1), "auth", "toplevel", "AUTH", "bob"},
	}
	if len(log) != len(want) {
		t.Fatalf("ACL LOG has %d entries, want %d: %#v", len(log), len(want), log)
	}
	for i, w := range want {
		e, _ := log[i].([]any)
		if len(e) != 20 {
			t.Fatalf("entry %d = %#v", i, e)
		}
		if got := []any{e[1], e[3], e[5], e[7], e[9]}; !slices.Equal(got, w) {
			t.Fatalf("entry %d = %#v, want %#v", i, got, w)
		}
	}
	if one, _ := admin.do("ACL", "LOG", "1").([]any); len(one) != 1 {
		t.Fatalf("ACL LOG 1 = %#v", one)
	}
	admin.expect(status("OK"), "ACL", "LOG", "RESET")
	admin.expect([]any{}, "ACL", "LOG")
}

func TestACL(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	admin := dial(t, addr)
	admin.expect("default", "ACL", "WHOAMI")
	admin.expect(errReply("ERR Error in ACL SETUSER modifier 'bogus': Syntax error"), "ACL", "SETUSER", "alice", "on", "bogus")
	admin.expect(errReply("ERR Error in ACL SETUSER modifier '+nosuch'"), "ACL", "SETUSER", "alice", "+nosuch")
	admin.expect([]any{"default"}, "ACL", "USERS")
	admin.expect(status("OK"), "ACL", "SETUSER", "alice", "on", ">s3cret", "~app:*", "+@read", "-@dangerous", "+set", "+multi", "+exec")
	admin.expect([]any{"alice", "default"}, "ACL", "USERS")

	alice := dial(t, addr)
	alice.expect(errReply("WRONGPASS"), "AUTH", "alice", "wrong")
	alice.expect(status("OK"), "AUTH", "alice", "s3cret")
	alice.expect("alice", "ACL", "WHOAMI")
	alice.expect(status("OK"), "SET", "app:1", "v")
	alice.expect("v", "GET", "app:1")
	alice.expect(errReply("NOPERM No permissions to access a key"), "SET", "other", "v")
	alice.expect(errReply("NOPERM No permissions to access a key"), "MGET", "app:1", "other")
	alice.expect(errReply("NOPERM User alice has no permissions to run the 'del' command"), "DEL", "app:1")
	alice.expect(errReply("NOPERM User alice has no permissions to run the 'keys' command"), "KEYS", "*")
	alice.expect(errReply("NOPERM User alice has no permissions to run the 'acl' command"), "ACL", "USERS")

	if _, err := io.WriteString(alice.conn, encode("SET", "app:2", "a")+encode("SET", "other", "b")); err != nil {
		t.Fatal(err)
	}
	if got := alice.read(); got != status("OK") {
		t.Fatalf("pipelined allowed SET = %#v", got)
	}
	if got, _ := alice.read().(errReply); !strings.HasPrefix(string(got), "NOPERM") {
		t.Fatalf("pipelined denied SET = %#v", got)
	}

	alice.expect(status("OK"), "MULTI")
	alice.expect(errReply("NOPERM"), "SET", "other", "v")
	alice.expect(errReply("EXECABORT"), "EXEC")

	hash := sha256.Sum256([]byte("s3cret"))
	list, _ := admin.do("ACL", "LIST").([]any)
	if !slices.Contains(list, any("user alice on #"+hex.EncodeToString(hash[:])+" ~app:* -@all +dbsize +exec +exists +get +mget +multi +pttl +scan +set +strlen +ttl +type")) ||
		!slices.Contains(list, any("user default on nopass ~* +@all")) {
		t.Fatalf("ACL LIST = %#v", list)
	}
	getuser, _ := admin.do("ACL", "GETUSER", "alice").([]any)
	if len(getuser) != 12 || getuser[5] == "+@all" || getuser[7] != "~app:*" {
		t.Fatalf("ACL GETUSER alice = %#v", getuser)
	}
	admin.expect(nil, "ACL", "GETUSER", "nobody")
	if cats, _ := admin.do("ACL", "CAT").([]any); !slices.Contains(cats, any("dangerous")) {
		t.Fatalf("ACL CAT = %#v", cats)
	}
	if strs, _ := admin.do("ACL", "CAT", "string").([]any); !slices.Contains(strs, any("get")) || slices.Contains(strs, any("del")) {
		t.Fatalf("ACL CAT string = %#v", strs)
	}
	admin.expect(errReply("ERR Unknown category"), "ACL", "CAT", "nosuch")

	admin.expect(status("OK"), "ACL", "SETUSER", "alice", "off")
	dial(t, addr).expect(errReply("WRONGPASS"), "AUTH", "alice", "s3cret")
	alice.expect("v", "GET", "app:1")

	admin.expect(errReply("ERR The 'default' user cannot be removed"), "ACL", "DELUSER", "default")
	admin.expect(int64(1), "ACL", "DELUSER", "alice", "nobody")
	if _, err := io.WriteString(alice.conn, encode("GET", "app:1")); err != nil {
		t.Fatal(err)
	}
	if _, err := alice.r.ReadByte(); err == nil {
		t.Fatal("a deleted user's connection stayed open")
	}

	admin.expect(status("OK"), "CONFIG", "SET", "requirepass", "pw")
	fresh := dial(t, addr)
	fresh.expect(errReply("NOAUTH"), "GET", "app:1")
	fresh.expect(status("OK"), "AUTH", "pw")
	fresh.expect("default", "ACL", "WHOAMI")
}
