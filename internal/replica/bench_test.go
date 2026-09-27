package replica

import (
	"slices"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/acneism/raft/node"
)

func benchTuning(c *node.Config) {
	c.TickInterval = time.Millisecond
}

func benchModes(b *testing.B, fn func(b *testing.B, l *testNode)) {
	for _, unsafe := range []bool{false, true} {
		b.Run("unsafe-no-fsync="+strconv.FormatBool(unsafe), func(b *testing.B) {
			tuning = benchTuning
			b.Cleanup(func() { tuning = testTuning })
			nodes := newCluster(b, 3, unsafe)
			l := leader(b, nodes)
			b.SetParallelism(16)
			b.ResetTimer()
			fn(b, l)
			b.StopTimer()
			start, target := time.Now(), l.node.Status().Applied
			eventually(b, "followers to catch up", func() bool {
				for _, tn := range nodes {
					if tn.node.Status().Applied < target {
						return false
					}
				}
				return true
			})
			b.ReportMetric(float64(time.Since(start).Microseconds())/1000, "catchup-ms")
		})
	}
}

func runTimed(b *testing.B, op func() error) {
	var mu sync.Mutex
	var lat []time.Duration
	b.RunParallel(func(pb *testing.PB) {
		var mine []time.Duration
		for pb.Next() {
			start := time.Now()
			if err := op(); err != nil {
				b.Error(err)
				return
			}
			mine = append(mine, time.Since(start))
		}
		mu.Lock()
		lat = append(lat, mine...)
		mu.Unlock()
	})
	if len(lat) == 0 {
		return
	}
	slices.Sort(lat)
	ms := func(p float64) float64 {
		return float64(lat[min(len(lat)-1, int(float64(len(lat))*p))].Microseconds()) / 1000
	}
	b.ReportMetric(ms(0.5), "p50-ms")
	b.ReportMetric(ms(0.99), "p99-ms")
}

func BenchmarkReplicatedSet(b *testing.B) {
	benchModes(b, func(b *testing.B, l *testNode) {
		var seq atomic.Int64
		runTimed(b, func() error { return put(l, strconv.FormatInt(seq.Add(1), 10), "value") })
	})
}

func BenchmarkReplicatedHotIncr(b *testing.B) {
	benchModes(b, func(b *testing.B, l *testNode) {
		runTimed(b, func() error { return incr(l, "hot") })
	})
}
