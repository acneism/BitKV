package replica

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"testing"
	"time"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/gliedabrennung/raft"
	"github.com/gliedabrennung/raft/node"
	"github.com/gliedabrennung/raft/transport"
	raftwal "github.com/gliedabrennung/raft/wal"
)

var ownTuning = testOwnTuning

func testOwnTuning(c *node.Config) {
	c.TickInterval = time.Millisecond
	c.SnapshotEntries = 16
	c.TrailingEntries = 4
	c.SegmentSize = 8 << 10
}

func benchOwnTuning(c *node.Config) {
	c.TickInterval = time.Millisecond
}

func ownEntry(index uint64, kind byte, ops ...bitcask.Op) raft.Entry {
	return raft.Entry{Index: index, Term: 1, Data: encodeOwnEntry(kind, ops)}
}

func TestOwnApplyKeepsLogOrder(t *testing.T) {
	dir := t.TempDir()
	db, err := bitcask.Open(filepath.Join(dir, "data"), bitcask.DefaultOptions())
	must(t, err)
	defer db.Close()
	f, err := newBitcaskFSM(db, filepath.Join(dir, "raft"))
	must(t, err)
	set := func(k, v string) bitcask.Op { return bitcask.Op{Key: k, Value: []byte(v)} }
	must(t, f.Apply([]raft.Entry{
		ownEntry(1, kindOps, set("a", "1"), set("b", "1"), set("x", "1")),
		{Index: 2, Term: 1, Type: raft.EntryNoop},
		ownEntry(3, kindOps, set("a", "2"), bitcask.Op{Key: "b", Delete: true}),
		ownEntry(4, kindOps, set("b", "3")),
		ownEntry(5, kindFlush),
		ownEntry(6, kindOps, set("a", "4")),
		ownEntry(7, kindOps, set("b", "5"), bitcask.Op{Key: "a", Delete: true}),
	}))
	tn := &testNode{db: db}
	for k, want := range map[string]string{"a": "", "b": "5", "x": ""} {
		if got := get(tn, k); got != want {
			t.Fatalf("%s = %q, want %q", k, got, want)
		}
	}
	if f.applied.Load() != 7 {
		t.Fatalf("applied %d, want 7", f.applied.Load())
	}
	if err := f.Apply([]raft.Entry{{Index: 8, Term: 1, Data: []byte{9}}}); !errors.Is(err, errEntry) {
		t.Fatalf("malformed entry: %v", err)
	}
}

func TestOwnRefusesOtherEngineDir(t *testing.T) {
	dir := t.TempDir()
	db, err := bitcask.Open(filepath.Join(dir, "data"), bitcask.DefaultOptions())
	must(t, err)
	defer db.Close()
	peers := map[string]string{"n0": freeAddr(t)}
	raftDir := filepath.Join(dir, "raft")
	must(t, os.MkdirAll(filepath.Join(raftDir, "wal"), 0o755))
	must(t, os.WriteFile(filepath.Join(raftDir, "wal", "wal-meta.db"), nil, 0o644))
	if _, err := Open(db, Config{ID: "n0", Peers: peers, Dir: raftDir, Engine: EngineOwn}); !errors.Is(err, ErrOtherEngine) {
		t.Fatalf("own engine over a hashicorp log: %v", err)
	}
	must(t, os.Remove(filepath.Join(raftDir, "wal", "wal-meta.db")))
	must(t, os.WriteFile(filepath.Join(raftDir, "wal", "meta"), nil, 0o644))
	if _, err := Open(db, Config{ID: "n0", Peers: peers, Dir: raftDir}); !errors.Is(err, ErrOtherEngine) {
		t.Fatalf("hashicorp engine over an own log: %v", err)
	}
}

func latestSnapshot(t *testing.T, raftDir string) (raft.SnapshotMeta, string) {
	t.Helper()
	des, err := os.ReadDir(filepath.Join(raftDir, "snap"))
	must(t, err)
	var best raft.SnapshotMeta
	var path string
	for _, de := range des {
		var term, index uint64
		if _, err := fmt.Sscanf(de.Name(), "%016x-%016x", &term, &index); err != nil || len(de.Name()) != 33 {
			continue
		}
		if index > best.Index {
			best, path = raft.SnapshotMeta{Index: index, Term: term}, filepath.Join(raftDir, "snap", de.Name())
		}
	}
	if path == "" {
		t.Fatal("no snapshot")
	}
	return best, path
}

func copyDir(t *testing.T, from, to string) {
	t.Helper()
	must(t, filepath.WalkDir(from, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, _ := filepath.Rel(from, path)
		if d.IsDir() {
			return os.MkdirAll(filepath.Join(to, rel), 0o755)
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(filepath.Join(to, rel), b, 0o644)
	}))
}

func TestOwnInterruptedRestoreResumes(t *testing.T) {
	nodes := newCluster(t, EngineOwn, 3, false)
	l := leader(t, nodes)
	keys := make([]string, 60)
	for i := range keys {
		keys[i] = "k" + strconv.Itoa(i)
		must(t, put(l, keys[i], "v"))
	}
	eventually(t, "replication", converged(nodes, "k59", "v", len(keys)))
	f := nodes[0]
	if f == l {
		f = nodes[1]
	}
	f.stop(t)
	for i := range 40 {
		must(t, put(l, "late"+strconv.Itoa(i), "v"))
	}
	snap, src := latestSnapshot(t, filepath.Join(l.dir, "raft"))
	var ids []raft.NodeID
	for id := range f.peers {
		ids = append(ids, raft.NodeID(id))
	}
	slices.Sort(ids)
	snap.Conf = raft.ConfState{Voters: ids}
	raftDir := filepath.Join(f.dir, "raft")
	copyDir(t, src, transport.IncomingDir(filepath.Join(raftDir, "snap"), snap))
	log, err := raftwal.Open(filepath.Join(raftDir, "wal"), snap.Conf, raftwal.Options{})
	must(t, err)
	if cur, _ := log.Snapshot(); cur.Index >= snap.Index {
		t.Fatalf("follower snapshot %d is not behind the leader's %d", cur.Index, snap.Index)
	}
	must(t, log.SetRestoring(&snap))
	must(t, log.Close())
	db, err := bitcask.Open(filepath.Join(f.dir, "data"), bitcask.DefaultOptions())
	must(t, err)
	must(t, db.Update(bitcask.Keys(keys[:30]...), func(tx *bitcask.Tx) error {
		for _, k := range keys[:30] {
			tx.Delete(k)
		}
		return nil
	}))
	must(t, db.Close())
	f.start(t)
	eventually(t, "interrupted restore to resume", converged(nodes, "late39", "v", len(keys)+40))
	f.stop(t)
	log, err = raftwal.Open(filepath.Join(raftDir, "wal"), snap.Conf, raftwal.Options{})
	must(t, err)
	defer log.Close()
	if log.Restoring() != nil {
		t.Fatal("restore marker left behind")
	}
}
