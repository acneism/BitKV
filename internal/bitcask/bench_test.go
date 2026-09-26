package bitcask

import (
	"bytes"
	"math/rand/v2"
	"strconv"
	"sync/atomic"
	"testing"
)

const benchKeys = 100000

func benchDB(b *testing.B, policy SyncPolicy) *DB {
	b.Helper()
	o := DefaultOptions()
	o.Sync = policy
	o.MergeInterval = 0
	db, err := Open(b.TempDir(), o)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { db.Close() })
	return db
}

func benchPut(b *testing.B, db *DB, key string, value []byte) {
	err := db.Update(Keys(key), func(tx *Tx) error {
		tx.Put(key, value, 0)
		return nil
	})
	if err != nil {
		b.Fatal(err)
	}
}

func BenchmarkPut(b *testing.B) {
	db := benchDB(b, SyncNo)
	value := bytes.Repeat([]byte("v"), 100)
	b.SetBytes(int64(len(value)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchPut(b, db, "key:"+strconv.Itoa(i%benchKeys), value)
	}
}

func BenchmarkPutSyncAlways(b *testing.B) {
	db := benchDB(b, SyncAlways)
	value := bytes.Repeat([]byte("v"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchPut(b, db, "key:"+strconv.Itoa(i%benchKeys), value)
	}
}

func BenchmarkPutSyncAlwaysParallel(b *testing.B) {
	db := benchDB(b, SyncAlways)
	value := bytes.Repeat([]byte("v"), 100)
	var n atomic.Int64
	b.SetParallelism(16)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := "key:" + strconv.FormatInt(n.Add(1)%benchKeys, 10)
			err := db.Update(Keys(key), func(tx *Tx) error {
				tx.Put(key, value, 0)
				return nil
			})
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkPutParallel(b *testing.B) {
	for _, logs := range []int{1, 4, 8} {
		b.Run("logs="+strconv.Itoa(logs), func(b *testing.B) {
			o := DefaultOptions()
			o.Sync = SyncNo
			o.MergeInterval = 0
			o.Logs = logs
			db, err := Open(b.TempDir(), o)
			if err != nil {
				b.Fatal(err)
			}
			b.Cleanup(func() { db.Close() })
			value := bytes.Repeat([]byte("v"), 100)
			var n atomic.Int64
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					key := "key:" + strconv.FormatInt(n.Add(1)%benchKeys, 10)
					err := db.Update(Keys(key), func(tx *Tx) error {
						tx.Put(key, value, 0)
						return nil
					})
					if err != nil {
						b.Error(err)
						return
					}
				}
			})
		})
	}
}

func BenchmarkCrossLogPairParallel(b *testing.B) {
	db := benchDB(b, SyncNo)
	value := bytes.Repeat([]byte("v"), 100)
	var n atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			i := n.Add(1)
			k1 := "a:" + strconv.FormatInt(i%benchKeys, 10)
			k2 := "b:" + strconv.FormatInt((i*7)%benchKeys, 10)
			err := db.Update(Keys(k1, k2), func(tx *Tx) error {
				tx.Put(k1, value, 0)
				tx.Put(k2, value, 0)
				return nil
			})
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkMixedParallel(b *testing.B) {
	db := loadedDB(b)
	value := bytes.Repeat([]byte("w"), 100)
	var seed atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewPCG(seed.Add(1), 11))
		for pb.Next() {
			key := "key:" + strconv.Itoa(rng.IntN(benchKeys))
			var err error
			if rng.IntN(10) == 0 {
				err = db.Update(Keys(key), func(tx *Tx) error {
					tx.Put(key, value, 0)
					return nil
				})
			} else {
				err = db.View(Keys(key), func(tx *Tx) error {
					_, _, err := tx.Get(key)
					return err
				})
			}
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkIncrParallel(b *testing.B) {
	db := benchDB(b, SyncNo)
	var n atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			key := "counter:" + strconv.FormatInt(n.Add(1)%1000, 10)
			err := db.Update(Keys(key), func(tx *Tx) error {
				v, _, err := tx.Get(key)
				if err != nil {
					return err
				}
				x, _ := strconv.Atoi(string(v))
				tx.Put(key, strconv.AppendInt(nil, int64(x+1), 10), 0)
				return nil
			})
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func loadedDB(b *testing.B) *DB {
	b.Helper()
	db := benchDB(b, SyncNo)
	value := bytes.Repeat([]byte("v"), 100)
	for i := range benchKeys {
		benchPut(b, db, "key:"+strconv.Itoa(i), value)
	}
	return db
}

func BenchmarkGet(b *testing.B) {
	db := loadedDB(b)
	rng := rand.New(rand.NewPCG(1, 2))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := "key:" + strconv.Itoa(rng.IntN(benchKeys))
		err := db.View(Keys(key), func(tx *Tx) error {
			_, _, err := tx.Get(key)
			return err
		})
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkGetParallel(b *testing.B) {
	db := loadedDB(b)
	var seed atomic.Uint64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		rng := rand.New(rand.NewPCG(seed.Add(1), 7))
		for pb.Next() {
			key := "key:" + strconv.Itoa(rng.IntN(benchKeys))
			err := db.View(Keys(key), func(tx *Tx) error {
				_, _, err := tx.Get(key)
				return err
			})
			if err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkMerge(b *testing.B) {
	db := loadedDB(b)
	value := bytes.Repeat([]byte("w"), 100)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		for k := 0; k < benchKeys; k += 2 {
			benchPut(b, db, "key:"+strconv.Itoa(k), value)
		}
		b.StartTimer()
		if err := db.Merge(); err != nil {
			b.Fatal(err)
		}
	}
}
