package main

import (
	"flag"
	"strings"
	"testing"
	"time"
)

func TestApplyEnv(t *testing.T) {
	fs := flag.NewFlagSet("casketdb", flag.ContinueOnError)
	addr := fs.String("addr", "127.0.0.1:6379", "")
	peers := fs.String("raft-peers", "", "")
	join := fs.Bool("raft-join", false, "")
	timeout := fs.Duration("raft-election-timeout", time.Second, "")
	if err := fs.Parse([]string{"-addr", "0.0.0.0:7000"}); err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"CASKETDB_ADDR":                  "10.0.0.1:6379",
		"CASKETDB_RAFT_PEERS":            "n1=a:1,n2=b:2",
		"CASKETDB_RAFT_JOIN":             "true",
		"CASKETDB_RAFT_ELECTION_TIMEOUT": "2s",
	}
	lookup := func(name string) (string, bool) {
		v, ok := env[name]
		return v, ok
	}
	shadowed, err := applyEnv(fs, lookup)
	if err != nil {
		t.Fatal(err)
	}
	if *addr != "0.0.0.0:7000" || *peers != "n1=a:1,n2=b:2" || !*join || *timeout != 2*time.Second {
		t.Fatalf("addr %q, peers %q, join %v, timeout %v", *addr, *peers, *join, *timeout)
	}
	if len(shadowed) != 1 || shadowed[0] != "CASKETDB_ADDR" {
		t.Fatalf("shadowed = %v, want [CASKETDB_ADDR]", shadowed)
	}
	fresh := flag.NewFlagSet("casketdb", flag.ContinueOnError)
	fresh.Duration("raft-election-timeout", time.Second, "")
	env["CASKETDB_RAFT_ELECTION_TIMEOUT"] = "soon"
	if _, err := applyEnv(fresh, lookup); err == nil || !strings.Contains(err.Error(), "CASKETDB_RAFT_ELECTION_TIMEOUT") {
		t.Fatalf("bad duration: %v", err)
	}
}
