package replica

import (
	"errors"
	"fmt"
	"net"
	"path/filepath"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/hashicorp/raft"
)

type testNode struct {
	id    string
	dir   string
	peers map[string]string
	db    *bitcask.DB
	node  *Node
}

var raftConfig = testRaftConfig

func testRaftConfig() *raft.Config {
	c := raft.DefaultConfig()
	c.HeartbeatTimeout = 50 * time.Millisecond
	c.ElectionTimeout = 50 * time.Millisecond
	c.LeaderLeaseTimeout = 50 * time.Millisecond
	c.CommitTimeout = 5 * time.Millisecond
	c.SnapshotInterval = 50 * time.Millisecond
	c.SnapshotThreshold = 16
	c.TrailingLogs = 4
	return c
}

func (tn *testNode) start(t testing.TB) {
	t.Helper()
	opts := bitcask.DefaultOptions()
	opts.Logs = 2
	db, err := bitcask.Open(filepath.Join(tn.dir, "data"), opts)
	if err != nil {
		t.Fatal(err)
	}
	n, err := open(db, Config{ID: tn.id, Peers: tn.peers, Dir: filepath.Join(tn.dir, "raft")}, raftConfig())
	if err != nil {
		db.Close()
		t.Fatal(err)
	}
	tn.db, tn.node = db, n
}

func (tn *testNode) stop(t testing.TB) {
	t.Helper()
	if tn.node == nil {
		return
	}
	if err := tn.node.Close(); err != nil {
		t.Error(err)
	}
	if err := tn.db.Close(); err != nil {
		t.Error(err)
	}
	tn.node = nil
}

func newCluster(t testing.TB, size int) []*testNode {
	peers := make(map[string]string)
	nodes := make([]*testNode, size)
	for i := range nodes {
		id := "n" + strconv.Itoa(i)
		peers[id] = freeAddr(t)
		nodes[i] = &testNode{id: id, dir: t.TempDir(), peers: peers}
	}
	for _, tn := range nodes {
		tn.start(t)
	}
	t.Cleanup(func() {
		for _, tn := range nodes {
			tn.stop(t)
		}
	})
	return nodes
}

func freeAddr(t testing.TB) string {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func eventually(t testing.TB, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(10 * time.Millisecond)
	}
}

func must(t testing.TB, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func leader(t testing.TB, nodes []*testNode) *testNode {
	t.Helper()
	var found *testNode
	eventually(t, "a ready leader", func() bool {
		for _, tn := range nodes {
			if tn.node == nil {
				continue
			}
			if _, _, err := tn.node.lease(); err == nil {
				found = tn
				return true
			}
		}
		return false
	})
	return found
}

func get(tn *testNode, key string) string {
	var v []byte
	tn.db.View(bitcask.Keys(key), func(tx *bitcask.Tx) error {
		v, _, _ = tx.Get(key)
		return nil
	})
	return string(v)
}

func put(tn *testNode, key, value string) error {
	return tn.node.Update(bitcask.Keys(key), func(tx *bitcask.Tx) error {
		tx.Put(key, []byte(value), 0)
		return nil
	})
}

func incr(tn *testNode, key string) error {
	return tn.node.Update(bitcask.Keys(key), func(tx *bitcask.Tx) error {
		v, _, err := tx.Get(key)
		if err != nil {
			return err
		}
		n, _ := strconv.Atoi(string(v))
		tx.Put(key, []byte(strconv.Itoa(n+1)), 0)
		return nil
	})
}

func converged(nodes []*testNode, key, want string, keys int) func() bool {
	return func() bool {
		for _, tn := range nodes {
			if tn.node != nil && (get(tn, key) != want || tn.db.Len() != keys) {
				return false
			}
		}
		return true
	}
}

func TestConcurrentWritesReplicate(t *testing.T) {
	nodes := newCluster(t, 3)
	l := leader(t, nodes)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for w := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				if err := incr(l, "counter"); err != nil {
					errs <- err
					return
				}
				if err := put(l, fmt.Sprintf("k%d-%d", w, i), "v"); err != nil {
					errs <- err
					return
				}
			}
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		t.Fatal(err)
	}
	eventually(t, "replicas to converge", converged(nodes, "counter", "400", 401))
	for _, tn := range nodes {
		if tn == l {
			continue
		}
		if err := put(tn, "x", "y"); !errors.Is(err, ErrNotLeader) {
			t.Fatalf("write on follower %s: %v", tn.id, err)
		}
	}
}

func TestCatchUpAndFailover(t *testing.T) {
	nodes := newCluster(t, 3)
	l := leader(t, nodes)
	for range 50 {
		must(t, incr(l, "counter"))
	}
	eventually(t, "initial replication", converged(nodes, "counter", "50", 1))
	lagging := nodes[0]
	if lagging == l {
		lagging = nodes[1]
	}
	lagging.stop(t)
	for i := range 100 {
		must(t, put(l, "k"+strconv.Itoa(i), "v"))
		must(t, incr(l, "counter"))
	}
	lagging.start(t)
	eventually(t, "restarted replica to catch up", converged(nodes, "counter", "150", 101))

	l.stop(t)
	next := leader(t, nodes)
	must(t, incr(next, "counter"))
	must(t, next.node.Flush())
	must(t, put(next, "after", "flush"))
	eventually(t, "flush to replicate", converged(nodes, "after", "flush", 1))
	l.start(t)
	eventually(t, "old leader to rejoin", converged(nodes, "after", "flush", 1))
}
