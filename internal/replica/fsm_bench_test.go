package replica

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/acneism/BitKV/internal/bitcask"
	ownraft "github.com/acneism/raft"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

func BenchmarkFollowerApply(b *testing.B) {
	for _, keys := range []int{1, 64} {
		b.Run("keys="+strconv.Itoa(keys), func(b *testing.B) {
			db, err := bitcask.Open(filepath.Join(b.TempDir(), "data"), bitcask.DefaultOptions())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			n := &Node{db: db}
			logs := make([]*raft.Log, b.N)
			for i := range logs {
				ops := make([]bitcask.Op, keys)
				for j := range ops {
					ops[j] = bitcask.Op{Key: strconv.Itoa(i*keys + j), Value: []byte("value")}
				}
				logs[i] = &raft.Log{Type: raft.LogCommand, Term: 1, Data: encodeEntry(kindOps, 1, uint64(i), ops)}
			}
			b.ResetTimer()
			applyAll(n, logs)
			b.ReportMetric(float64(b.N*keys)/b.Elapsed().Seconds(), "keys/s")
		})
	}
}

func applyAll(n *Node, logs []*raft.Log) {
	batch := raft.DefaultConfig().MaxAppendEntries
	for len(logs) > 0 {
		k := min(batch, len(logs))
		n.ApplyBatch(logs[:k])
		logs = logs[k:]
	}
}

func BenchmarkSnapshotPersist(b *testing.B) {
	dir := b.TempDir()
	opts := bitcask.DefaultOptions()
	opts.Sync = bitcask.SyncNo
	db, err := bitcask.Open(filepath.Join(dir, "data"), opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := make([]byte, 100)
	for i := 0; i < 200_000; i += 1000 {
		ops := make([]bitcask.Op, 1000)
		for j := range ops {
			ops[j] = bitcask.Op{Key: strconv.Itoa(i + j), Value: value}
		}
		if err := db.Apply(ops, 0); err != nil {
			b.Fatal(err)
		}
	}
	store, err := newLinkStore(filepath.Join(dir, "raft"), hclog.NewNullLogger())
	if err != nil {
		b.Fatal(err)
	}
	n := &Node{db: db}
	b.ResetTimer()
	for i := range b.N {
		sink, err := store.Create(raft.SnapshotVersionMax, uint64(i+1), 1, raft.Configuration{}, 1, nil)
		if err != nil {
			b.Fatal(err)
		}
		snap, _ := n.Snapshot()
		if err := snap.Persist(sink); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkOwnFollowerApply(b *testing.B) {
	for _, keys := range []int{1, 64} {
		b.Run("keys="+strconv.Itoa(keys), func(b *testing.B) {
			dir := b.TempDir()
			db, err := bitcask.Open(filepath.Join(dir, "data"), bitcask.DefaultOptions())
			if err != nil {
				b.Fatal(err)
			}
			defer db.Close()
			f, err := newBitcaskFSM(db, filepath.Join(dir, "raft"))
			if err != nil {
				b.Fatal(err)
			}
			ents := make([]ownraft.Entry, b.N)
			for i := range ents {
				ops := make([]bitcask.Op, keys)
				for j := range ops {
					ops[j] = bitcask.Op{Key: strconv.Itoa(i*keys + j), Value: []byte("value")}
				}
				ents[i] = ownraft.Entry{Index: uint64(i + 1), Term: 1, Data: encodeOwnEntry(kindOps, ops)}
			}
			b.ResetTimer()
			for len(ents) > 0 {
				k := min(raft.DefaultConfig().MaxAppendEntries, len(ents))
				if err := f.Apply(ents[:k]); err != nil {
					b.Fatal(err)
				}
				ents = ents[k:]
			}
			b.ReportMetric(float64(b.N*keys)/b.Elapsed().Seconds(), "keys/s")
		})
	}
}

func BenchmarkOwnSnapshotPersist(b *testing.B) {
	dir := b.TempDir()
	opts := bitcask.DefaultOptions()
	opts.Sync = bitcask.SyncNo
	db, err := bitcask.Open(filepath.Join(dir, "data"), opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	value := make([]byte, 100)
	for i := 0; i < 200_000; i += 1000 {
		ops := make([]bitcask.Op, 1000)
		for j := range ops {
			ops[j] = bitcask.Op{Key: strconv.Itoa(i + j), Value: value}
		}
		if err := db.Apply(ops, 0); err != nil {
			b.Fatal(err)
		}
	}
	f, err := newBitcaskFSM(db, filepath.Join(dir, "raft"))
	if err != nil {
		b.Fatal(err)
	}
	b.ResetTimer()
	for i := range b.N {
		if _, err := f.Snapshot(filepath.Join(dir, "snap", strconv.Itoa(i))); err != nil {
			b.Fatal(err)
		}
	}
}
