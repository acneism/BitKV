package replica

import (
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

type slowApply struct {
	node.StateMachine
	delay time.Duration
}

func (s slowApply) Apply(ents []raft.Entry) error {
	time.Sleep(s.delay)
	return s.StateMachine.Apply(ents)
}

func withReads(mode ReadMode) func(*testNode) {
	return func(tn *testNode) { tn.reads = mode }
}

func withSlowApply(delay time.Duration) func(*testNode) {
	return func(tn *testNode) {
		tn.tune = func(c *node.Config) { c.StateMachine = slowApply{c.StateMachine, delay} }
	}
}

func eachReadMode(t *testing.T, fn func(t *testing.T, mode ReadMode)) {
	for name, mode := range map[string]ReadMode{"linearizable": ReadLinearizable, "lease": ReadLease} {
		t.Run(name, func(t *testing.T) { fn(t, mode) })
	}
}

func consistentGet(t *testing.T, tn *testNode, key string) string {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		err := tn.node.ReadBarrier()
		if err == nil {
			return get(tn, key)
		}
		if !errors.Is(err, ErrUnconfirmed) || time.Now().After(deadline) {
			t.Fatal(err)
		}
	}
}

func TestParseReadMode(t *testing.T) {
	for s, want := range map[string]ReadMode{"local": ReadLocal, "linearizable": ReadLinearizable, "lease": ReadLease} {
		if got, err := ParseReadMode(s); err != nil || got != want {
			t.Fatalf("ParseReadMode(%q) = %v, %v", s, got, err)
		}
	}
	if _, err := ParseReadMode("stale"); err == nil {
		t.Fatal("an unknown read mode was accepted")
	}
}

func TestConsistentReadsSeeTheLatestWrite(t *testing.T) {
	eachReadMode(t, func(t *testing.T, mode ReadMode) {
		nodes := newCluster(t, 3, false, withReads(mode), withSlowApply(10*time.Millisecond))
		l := leader(t, nodes)
		f := follower(nodes, l)
		for i := range 50 {
			want := strconv.Itoa(i)
			must(t, put(l, "k", want))
			if got := consistentGet(t, f, "k"); got != want {
				t.Fatalf("follower read %q right after the write of %q", got, want)
			}
			if got := consistentGet(t, l, "k"); got != want {
				t.Fatalf("leader read %q right after the write of %q", got, want)
			}
		}
	})
}

func TestConsistentReadsSkipProposedValues(t *testing.T) {
	eachReadMode(t, func(t *testing.T, mode ReadMode) {
		nodes := newCluster(t, 3, false, withReads(mode))
		l := leader(t, nodes)
		must(t, put(l, "k", "committed"))
		_, err := l.db.Propose(bitcask.Keys("k"), l.node.ready.Load(), func(tx *bitcask.Tx) error {
			tx.Put("k", []byte("proposed"), 0)
			return nil
		}, func([]bitcask.Op) (uint64, error) { return 1 << 62, nil })
		must(t, err)
		var seen string
		must(t, l.node.Update(bitcask.Keys("k", "other"), func(tx *bitcask.Tx) error {
			v, _, err := tx.Get("k")
			seen = string(v)
			tx.Put("other", []byte("v"), 0)
			return err
		}))
		if seen != "proposed" {
			t.Fatalf("a writer of the same term saw %q, the proposed value was not staged", seen)
		}
		for _, tn := range nodes {
			if got := consistentGet(t, tn, "k"); got != "committed" {
				t.Fatalf("%s read %q, want the committed value", tn.id, got)
			}
		}
	})
}

func TestIsolatedNodeRefusesConsistentReads(t *testing.T) {
	eachReadMode(t, func(t *testing.T, mode ReadMode) {
		for _, isolated := range []string{"leader", "follower"} {
			t.Run(isolated, func(t *testing.T) {
				nodes := newCluster(t, 3, false, withReads(mode))
				l := leader(t, nodes)
				must(t, put(l, "k", "v"))
				keep := l
				if isolated == "follower" {
					keep = follower(nodes, l)
					eventually(t, "the follower to apply the write", func() bool { return get(keep, "k") == "v" })
				}
				for _, tn := range nodes {
					if tn != keep {
						tn.stop(t)
					}
				}
				if mode == ReadLinearizable {
					if err := keep.node.ReadBarrier(); !errors.Is(err, ErrUnconfirmed) {
						t.Fatalf("read on an isolated %s: %v, want ErrUnconfirmed", isolated, err)
					}
					return
				}
				eventually(t, "the lease to run out", func() bool {
					return errors.Is(keep.node.ReadBarrier(), ErrUnconfirmed)
				})
			})
		}
	})
}

func TestLocalReadsNeedNoLeader(t *testing.T) {
	nodes := newCluster(t, 3, false)
	l := leader(t, nodes)
	must(t, put(l, "k", "v"))
	f := follower(nodes, l)
	eventually(t, "the follower to apply the write", func() bool { return get(f, "k") == "v" })
	for _, tn := range nodes {
		if tn != f {
			tn.stop(t)
		}
	}
	if got := consistentGet(t, f, "k"); got != "v" {
		t.Fatalf("local read on a lone follower = %q", got)
	}
}
