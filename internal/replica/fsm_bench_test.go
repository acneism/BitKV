package replica

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/acneism/raft"
)

func BenchmarkFollowerApply(b *testing.B) {
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
			ents := make([]raft.Entry, b.N)
			for i := range ents {
				ops := make([]bitcask.Op, keys)
				for j := range ops {
					ops[j] = bitcask.Op{Key: strconv.Itoa(i*keys + j), Value: []byte("value")}
				}
				ents[i] = entry(uint64(i+1), kindOps, ops...)
			}
			b.ResetTimer()
			for len(ents) > 0 {
				k := min(64, len(ents))
				if err := f.Apply(ents[:k]); err != nil {
					b.Fatal(err)
				}
				ents = ents[k:]
			}
			b.ReportMetric(float64(b.N*keys)/b.Elapsed().Seconds(), "keys/s")
		})
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
