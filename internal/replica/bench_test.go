package replica

import (
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/hashicorp/raft"
)

func benchRaftConfig() *raft.Config {
	c := testRaftConfig()
	c.SnapshotThreshold = snapshotThreshold
	c.SnapshotInterval = raft.DefaultConfig().SnapshotInterval
	c.TrailingLogs = raft.DefaultConfig().TrailingLogs
	return c
}

func benchModes(b *testing.B, fn func(b *testing.B, l *testNode)) {
	for _, unsafe := range []bool{false, true} {
		b.Run("unsafe-no-fsync="+strconv.FormatBool(unsafe), func(b *testing.B) {
			raftConfig = benchRaftConfig
			b.Cleanup(func() { raftConfig = testRaftConfig })
			l := leader(b, newCluster(b, 3, unsafe))
			b.SetParallelism(16)
			b.ResetTimer()
			fn(b, l)
		})
	}
}

func BenchmarkReplicatedSet(b *testing.B) {
	benchModes(b, func(b *testing.B, l *testNode) {
		var seq atomic.Int64
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := put(l, strconv.FormatInt(seq.Add(1), 10), "value"); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}

func BenchmarkReplicatedHotIncr(b *testing.B) {
	benchModes(b, func(b *testing.B, l *testNode) {
		b.RunParallel(func(pb *testing.PB) {
			for pb.Next() {
				if err := incr(l, "hot"); err != nil {
					b.Error(err)
					return
				}
			}
		})
	})
}
