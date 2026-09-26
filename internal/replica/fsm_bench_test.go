package replica

import (
	"path/filepath"
	"strconv"
	"testing"

	"github.com/acneism/BitKV/internal/bitcask"
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
			n := &Node{db: db, pending: make(map[uint64]*proposal)}
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
