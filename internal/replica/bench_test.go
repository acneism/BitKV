package replica

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/raft"
)

func benchRaftConfig() *raft.Config {
	c := testRaftConfig()
	c.SnapshotThreshold = raft.DefaultConfig().SnapshotThreshold
	c.SnapshotInterval = raft.DefaultConfig().SnapshotInterval
	c.TrailingLogs = raft.DefaultConfig().TrailingLogs
	return c
}

func benchCluster(b *testing.B) *testNode {
	raftConfig = benchRaftConfig
	b.Cleanup(func() { raftConfig = testRaftConfig })
	nodes := newCluster(b, 3)
	return leader(b, nodes)
}

func BenchmarkReplicatedSet(b *testing.B) {
	l := benchCluster(b)
	var seq atomic.Int64
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := put(l, strconv.FormatInt(seq.Add(1), 10), "value"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkReplicatedHotIncr(b *testing.B) {
	l := benchCluster(b)
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := incr(l, "hot"); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
