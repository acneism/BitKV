package bitcask

import (
	"bytes"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"bitkv/internal/clock"
)

func testOptions() Options {
	o := DefaultOptions()
	o.Sync = SyncNo
	o.MaxFileSize = 4 << 10
	o.MergeInterval = 0
	o.ExpireInterval = 0
	o.Logs = 1
	return o
}

func logDir(dir string) string {
	return groupDir(dir, 0)
}

func mustOpen(t *testing.T, dir string, o Options) *DB {
	t.Helper()
	db, err := Open(dir, o)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	return db
}

func mustClose(t *testing.T, db *DB) {
	t.Helper()
	if err := db.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}
}

func put(t *testing.T, db *DB, key, value string, expireAt int64) {
	t.Helper()
	err := db.Update(Keys(key), func(tx *Tx) error {
		tx.Put(key, []byte(value), expireAt)
		return nil
	})
	if err != nil {
		t.Fatalf("put %q: %v", key, err)
	}
}

func del(t *testing.T, db *DB, key string) bool {
	t.Helper()
	var deleted bool
	err := db.Update(Keys(key), func(tx *Tx) error {
		deleted = tx.Delete(key)
		return nil
	})
	if err != nil {
		t.Fatalf("delete %q: %v", key, err)
	}
	return deleted
}

func get(t *testing.T, db *DB, key string) (string, bool) {
	t.Helper()
	var value []byte
	var found bool
	err := db.View(Keys(key), func(tx *Tx) error {
		var err error
		value, found, err = tx.Get(key)
		return err
	})
	if err != nil {
		t.Fatalf("get %q: %v", key, err)
	}
	return string(value), found
}

func expect(t *testing.T, db *DB, key, want string) {
	t.Helper()
	if got, ok := get(t, db, key); !ok || got != want {
		t.Fatalf("get %q = %q, %v; want %q", key, got, ok, want)
	}
}

func expectMissing(t *testing.T, db *DB, key string) {
	t.Helper()
	if got, ok := get(t, db, key); ok {
		t.Fatalf("get %q = %q; want missing", key, got)
	}
}

func dataIDs(t *testing.T, dir string) []uint32 {
	t.Helper()
	ids, err := listIDs(logDir(dir), dataExt)
	if err != nil {
		t.Fatal(err)
	}
	return ids
}

func lastDataFile(t *testing.T, dir string) string {
	t.Helper()
	ids := dataIDs(t, dir)
	if len(ids) == 0 {
		t.Fatal("no data files")
	}
	return filepath.Join(logDir(dir), fileName(ids[len(ids)-1], dataExt))
}

func copyFiles(t *testing.T, src, dst string, keep func(name string) bool) {
	t.Helper()
	entries, err := os.ReadDir(src)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if e.IsDir() || e.Name() == lockName || !keep(e.Name()) {
			continue
		}
		data, err := os.ReadFile(filepath.Join(src, e.Name()))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, e.Name()), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func TestPutGetDelete(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	put(t, db, "a", "1", 0)
	expect(t, db, "a", "1")
	put(t, db, "a", "2", 0)
	expect(t, db, "a", "2")
	if !del(t, db, "a") {
		t.Fatal("delete of existing key returned false")
	}
	expectMissing(t, db, "a")
	if del(t, db, "a") {
		t.Fatal("delete of missing key returned true")
	}
	if n := db.Len(); n != 0 {
		t.Fatalf("len = %d, want 0", n)
	}
}

func TestReopen(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for i := range 500 {
		put(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 0)
	}
	for i := 0; i < 500; i += 3 {
		del(t, db, fmt.Sprintf("k%d", i))
	}
	if db.Stats().DataFiles < 2 {
		t.Fatal("expected file rotation")
	}
	mustClose(t, db)

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	for i := range 500 {
		key := fmt.Sprintf("k%d", i)
		if i%3 == 0 {
			expectMissing(t, db, key)
		} else {
			expect(t, db, key, fmt.Sprintf("v%d", i))
		}
	}
}

func TestTornTailIsTruncated(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	put(t, db, "a", "1", 0)
	put(t, db, "b", "2", 0)
	mustClose(t, db)

	f, err := os.OpenFile(lastDataFile(t, dir), os.O_WRONLY|os.O_APPEND, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write(bytes.Repeat([]byte{0xAB}, 64)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	db = mustOpen(t, dir, testOptions())
	expect(t, db, "a", "1")
	expect(t, db, "b", "2")
	put(t, db, "c", "3", 0)
	mustClose(t, db)

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	expect(t, db, "c", "3")
}

func TestPartialBatchIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.MaxFileSize = 1 << 20
	db := mustOpen(t, dir, o)
	put(t, db, "before", "ok", 0)
	err := db.Update(Keys("x", "y", "z"), func(tx *Tx) error {
		tx.Put("x", []byte("1"), 0)
		tx.Put("y", []byte("2"), 0)
		tx.Put("z", []byte("3"), 0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	mustClose(t, db)

	path := lastDataFile(t, dir)
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, st.Size()-2); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	expect(t, db, "before", "ok")
	for _, key := range []string{"x", "y", "z"} {
		expectMissing(t, db, key)
	}
	st, err = os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if want := recordSize(len("before"), len("ok")); st.Size() != want {
		t.Fatalf("file size after recovery = %d, want %d", st.Size(), want)
	}
}

func manualOptions() (Options, *clock.Manual) {
	clk := clock.NewManual(time.Now())
	o := testOptions()
	o.Now = clk.Now
	return o, clk
}

func at(clk *clock.Manual, d time.Duration) int64 {
	return clk.Now().Add(d).UnixMilli()
}

func TestExpiredValueDoesNotResurrect(t *testing.T) {
	dir := t.TempDir()
	o, clk := manualOptions()
	db := mustOpen(t, dir, o)
	put(t, db, "k", "old", 0)
	put(t, db, "k", "new", at(clk, 50*time.Millisecond))
	expect(t, db, "k", "new")
	clk.Advance(49 * time.Millisecond)
	expect(t, db, "k", "new")
	clk.Advance(time.Millisecond)
	expectMissing(t, db, "k")
	mustClose(t, db)

	db = mustOpen(t, dir, o)
	expectMissing(t, db, "k")
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	expectMissing(t, db, "k")
	mustClose(t, db)

	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	expectMissing(t, db, "k")
}

func TestMergeDropsExpiredKeys(t *testing.T) {
	o, clk := manualOptions()
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	put(t, db, "short", "v", at(clk, 30*time.Millisecond))
	put(t, db, "long", "v", 0)
	clk.Advance(time.Second)
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	if n := db.Len(); n != 1 {
		t.Fatalf("len after merge = %d, want 1", n)
	}
	expect(t, db, "long", "v")
}

func TestActiveExpirySweepsSparseTTLKeys(t *testing.T) {
	for _, long := range []int{50, 5000} {
		t.Run(fmt.Sprintf("long=%d", long), func(t *testing.T) {
			o, clk := manualOptions()
			o.MaxFileSize = 64 << 20
			db := mustOpen(t, t.TempDir(), o)
			defer mustClose(t, db)
			const plain, short = 20000, 50
			err := db.Update(All(), func(tx *Tx) error {
				for i := range plain {
					tx.Put(fmt.Sprintf("plain:%d", i), []byte("v"), 0)
				}
				for i := range short {
					tx.Put(fmt.Sprintf("short:%d", i), []byte("v"), at(clk, time.Second))
				}
				for i := range long {
					tx.Put(fmt.Sprintf("long:%d", i), []byte("v"), at(clk, time.Hour))
				}
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			clk.Advance(2 * time.Second)
			want := plain + long
			cycles := 0
			for db.Len() > want {
				if cycles++; cycles > numShards {
					t.Fatalf("after %d cycles %d keys remain, want %d", numShards, db.Len(), want)
				}
				db.expireCycle()
			}
			st := db.Stats()
			t.Logf("expired %d keys in %d cycles", st.ExpiredKeys, cycles)
			if st.ExpiredKeys != short || st.KeysWithTTL != long {
				t.Fatalf("stats = %+v, want %d expired and %d keys with TTL left", st, short, long)
			}
			for i := range short {
				expectMissing(t, db, fmt.Sprintf("short:%d", i))
			}
			expect(t, db, "long:0", "v")
			if maxCycles := numShards / expireShardsPerCycle; cycles > maxCycles {
				t.Fatalf("sweep took %d cycles, want at most %d", cycles, maxCycles)
			}
		})
	}
}

func TestActiveExpiryIgnoresRenewedKeys(t *testing.T) {
	o, clk := manualOptions()
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	put(t, db, "k", "old", at(clk, time.Second))
	clk.Advance(2 * time.Second)
	put(t, db, "k", "renewed", at(clk, time.Hour))
	for range numShards {
		db.expireCycle()
	}
	expect(t, db, "k", "renewed")
	if st := db.Stats(); st.ExpiredKeys != 0 {
		t.Fatalf("expired %d keys, want 0", st.ExpiredKeys)
	}
}

func TestMergeCompacts(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for round := range 10 {
		for i := range 100 {
			put(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d-%d", i, round), 0)
		}
	}
	for i := 0; i < 100; i += 2 {
		del(t, db, fmt.Sprintf("k%d", i))
	}
	before := db.Stats()
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	after := db.Stats()
	if after.TotalBytes >= before.TotalBytes/2 {
		t.Fatalf("merge did not compact: %d -> %d bytes", before.TotalBytes, after.TotalBytes)
	}
	if after.LiveBytes != before.LiveBytes || after.Keys != before.Keys {
		t.Fatalf("live data changed: %+v -> %+v", before, after)
	}
	check := func(db *DB) {
		t.Helper()
		for i := range 100 {
			key := fmt.Sprintf("k%d", i)
			if i%2 == 0 {
				expectMissing(t, db, key)
			} else {
				expect(t, db, key, fmt.Sprintf("v%d-9", i))
			}
		}
	}
	check(db)
	if hints, _ := listIDs(logDir(dir), hintExt); len(hints) == 0 {
		t.Fatal("no hint files after merge")
	}
	if _, err := os.Stat(filepath.Join(logDir(dir), mergeDirName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("merge directory left behind: %v", err)
	}
	mustClose(t, db)

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	check(db)
	put(t, db, "k1", "fresh", 0)
	expect(t, db, "k1", "fresh")
}

func TestMergeWithConcurrentWrites(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	var mu sync.Mutex
	model := map[string]string{}
	done := make(chan error, 1)
	go func() {
		for i := range 3000 {
			key := fmt.Sprintf("k%d", i%64)
			value := fmt.Sprintf("v%d", i)
			err := db.Update(Keys(key), func(tx *Tx) error {
				tx.Put(key, []byte(value), 0)
				return nil
			})
			if err != nil {
				done <- err
				return
			}
			mu.Lock()
			model[key] = value
			mu.Unlock()
		}
		done <- nil
	}()
	merges := 0
	for running := true; running; {
		select {
		case err := <-done:
			if err != nil {
				t.Fatal(err)
			}
			running = false
		default:
		}
		if err := db.Merge(); err != nil {
			t.Fatal(err)
		}
		merges++
	}
	if merges < 2 {
		t.Fatalf("only %d merges ran", merges)
	}
	verify := func(db *DB) {
		t.Helper()
		for k, v := range model {
			expect(t, db, k, v)
		}
		if db.Len() != len(model) {
			t.Fatalf("len = %d, want %d", db.Len(), len(model))
		}
	}
	verify(db)
	mustClose(t, db)
	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	verify(db)
}

func TestMergeCompletesAfterCrash(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for i := range 300 {
		put(t, db, fmt.Sprintf("k%d", i%50), fmt.Sprintf("v%d", i), 0)
	}
	for i := 0; i < 50; i += 5 {
		del(t, db, fmt.Sprintf("k%d", i))
	}
	mustClose(t, db)
	ids := dataIDs(t, dir)
	boundary := ids[len(ids)-1]

	scratch := t.TempDir()
	copyFiles(t, dir, scratch, func(string) bool { return true })
	if err := os.MkdirAll(logDir(scratch), 0o755); err != nil {
		t.Fatal(err)
	}
	copyFiles(t, logDir(dir), logDir(scratch), func(string) bool { return true })
	sdb := mustOpen(t, scratch, testOptions())
	if err := sdb.Merge(); err != nil {
		t.Fatal(err)
	}
	mustClose(t, sdb)

	mergeDir := filepath.Join(logDir(dir), mergeDirName)
	if err := os.MkdirAll(mergeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	copyFiles(t, logDir(scratch), mergeDir, func(name string) bool {
		base := strings.TrimSuffix(strings.TrimSuffix(name, dataExt), hintExt)
		return fileExists(filepath.Join(logDir(scratch), base+hintExt))
	})
	if err := writeMarker(mergeDir, boundary); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	for i := range 50 {
		key := fmt.Sprintf("k%d", i)
		if i%5 == 0 {
			expectMissing(t, db, key)
		} else {
			expect(t, db, key, fmt.Sprintf("v%d", 250+i))
		}
	}
	for _, id := range dataIDs(t, dir) {
		if id <= boundary {
			t.Fatalf("old data file %d was not removed", id)
		}
	}
	if _, err := os.Stat(mergeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("merge directory left behind: %v", err)
	}
}

func TestIncompleteMergeIsDiscarded(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	put(t, db, "a", "1", 0)
	mustClose(t, db)
	mergeDir := filepath.Join(logDir(dir), mergeDirName)
	if err := os.MkdirAll(mergeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(mergeDir, fileName(99, dataExt)), []byte("garbage"), 0o644); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	expect(t, db, "a", "1")
	if _, err := os.Stat(mergeDir); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("incomplete merge directory left behind: %v", err)
	}
	if fileExists(filepath.Join(logDir(dir), fileName(99, dataExt))) {
		t.Fatal("file from incomplete merge was moved into the database")
	}
}

func TestFlushPersists(t *testing.T) {
	for _, logs := range []int{1, 4} {
		t.Run(fmt.Sprintf("logs=%d", logs), func(t *testing.T) {
			dir := t.TempDir()
			o := testOptions()
			o.Logs = logs
			db := mustOpen(t, dir, o)
			for i := range 20 {
				put(t, db, fmt.Sprintf("old%d", i), "1", 0)
			}
			if err := db.Flush(); err != nil {
				t.Fatal(err)
			}
			expectMissing(t, db, "old0")
			put(t, db, "c", "3", 0)
			mustClose(t, db)

			db = mustOpen(t, dir, o)
			defer mustClose(t, db)
			for i := range 20 {
				expectMissing(t, db, fmt.Sprintf("old%d", i))
			}
			expect(t, db, "c", "3")
			if n := db.Len(); n != 1 {
				t.Fatalf("len = %d, want 1", n)
			}
			if fileExists(filepath.Join(dir, flushName)) {
				t.Fatal("flush marker left behind")
			}
		})
	}
}

func TestFlushCompletesAfterCrash(t *testing.T) {
	dir := t.TempDir()
	saved := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	put(t, db, "a", "1", 0)
	put(t, db, "b", "2", 0)
	mustClose(t, db)
	ids := dataIDs(t, dir)
	bound := ids[len(ids)-1]
	copyFiles(t, logDir(dir), saved, func(string) bool { return true })

	db = mustOpen(t, dir, testOptions())
	if err := db.Flush(); err != nil {
		t.Fatal(err)
	}
	put(t, db, "c", "3", 0)
	mustClose(t, db)
	copyFiles(t, saved, logDir(dir), func(string) bool { return true })
	if err := writeFlushMarker(dir, []uint32{bound}); err != nil {
		t.Fatal(err)
	}

	db = mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	expectMissing(t, db, "a")
	expectMissing(t, db, "b")
	expect(t, db, "c", "3")
	for _, id := range dataIDs(t, dir) {
		if id <= bound {
			t.Fatalf("file %d from before FLUSH was not removed", id)
		}
	}
	if fileExists(filepath.Join(dir, flushName)) {
		t.Fatal("flush marker left behind")
	}
}

func TestSecondOpenIsLocked(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	defer mustClose(t, db)
	if _, err := Open(dir, testOptions()); !errors.Is(err, ErrLocked) {
		t.Fatalf("second open: err = %v, want ErrLocked", err)
	}
}

func TestCorruptionInOlderFileFails(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, testOptions())
	for i := range 300 {
		put(t, db, fmt.Sprintf("k%d", i), fmt.Sprintf("v%d", i), 0)
	}
	mustClose(t, db)
	ids := dataIDs(t, dir)
	if len(ids) < 2 {
		t.Fatal("expected several data files")
	}
	path := filepath.Join(logDir(dir), fileName(ids[0], dataExt))
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if err := os.WriteFile(path, data, 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir, testOptions()); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("open corrupted db: err = %v, want ErrCorrupt", err)
	}
}

func TestReadDetectsCorruption(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.MaxFileSize = 1 << 20
	db := mustOpen(t, dir, o)
	defer mustClose(t, db)
	put(t, db, "k", "value", 0)
	g := db.groups[0]
	g.logMu.Lock()
	df := g.active
	g.logMu.Unlock()
	if _, err := df.f.WriteAt([]byte("X"), headerSize+1); err != nil {
		t.Fatal(err)
	}
	err := db.View(Keys("k"), func(tx *Tx) error {
		_, _, err := tx.Get("k")
		return err
	})
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("read of corrupted record: err = %v, want ErrCorrupt", err)
	}
}

func TestScanVisitsEveryKeyOnce(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	for i := range 1000 {
		put(t, db, fmt.Sprintf("key:%d", i), "v", 0)
	}
	seen := map[string]int{}
	var cursor uint64
	for {
		next, keys := db.Scan(cursor, 50, nil)
		for _, k := range keys {
			seen[k]++
		}
		if next == 0 {
			break
		}
		cursor = next
	}
	if len(seen) != 1000 {
		t.Fatalf("scan saw %d keys, want 1000", len(seen))
	}
	for k, n := range seen {
		if n != 1 {
			t.Fatalf("key %q returned %d times", k, n)
		}
	}
	matched := db.Keys(func(k string) bool { return strings.HasSuffix(k, "7") })
	if len(matched) != 100 {
		t.Fatalf("keys ending in 7: %d, want 100", len(matched))
	}
}

func TestSyncAlwaysConcurrentWriters(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.Sync = SyncAlways
	db := mustOpen(t, dir, o)
	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for g := range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range 50 {
				key := fmt.Sprintf("g%d-%d", g, i)
				err := db.Update(Keys(key), func(tx *Tx) error {
					tx.Put(key, []byte("v"), 0)
					return nil
				})
				if err != nil {
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
	mustClose(t, db)
	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	if n := db.Len(); n != 400 {
		t.Fatalf("len = %d, want 400", n)
	}
}

func TestConcurrentReadModifyWrite(t *testing.T) {
	for _, policy := range []SyncPolicy{SyncNo, SyncAlways} {
		t.Run(policy.String(), func(t *testing.T) {
			dir := t.TempDir()
			o := testOptions()
			o.Sync = policy
			db := mustOpen(t, dir, o)
			var wg sync.WaitGroup
			errs := make(chan error, 8)
			for g := range 8 {
				wg.Add(1)
				go func() {
					defer wg.Done()
					for i := range 150 {
						key := fmt.Sprintf("counter%d", (g+i)%4)
						err := db.Update(Keys(key), func(tx *Tx) error {
							v, _, err := tx.Get(key)
							if err != nil {
								return err
							}
							n := 0
							if len(v) > 0 {
								fmt.Sscan(string(v), &n)
							}
							tx.Put(key, []byte(fmt.Sprint(n+1)), 0)
							return nil
						})
						if err != nil {
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
			check := func(db *DB) {
				t.Helper()
				for k := range 4 {
					expect(t, db, fmt.Sprintf("counter%d", k), "300")
				}
			}
			check(db)
			mustClose(t, db)
			db = mustOpen(t, dir, o)
			defer mustClose(t, db)
			check(db)
		})
	}
}

func TestGroupCommitSharesFsyncs(t *testing.T) {
	o := testOptions()
	o.Sync = SyncAlways
	o.MaxFileSize = 64 << 20
	db := mustOpen(t, t.TempDir(), o)
	defer mustClose(t, db)
	const writers, perWriter = 16, 30
	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				key := fmt.Sprintf("g%d-%d", g, i)
				err := db.Update(Keys(key), func(tx *Tx) error {
					tx.Put(key, []byte("v"), 0)
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	wg.Wait()
	st := db.Stats()
	t.Logf("%d commits, %d writes, %d fsyncs", writers*perWriter, st.Writes, st.Fsyncs)
	if st.Fsyncs >= writers*perWriter || st.Writes >= writers*perWriter {
		t.Fatalf("no batching: %d writes and %d fsyncs for %d commits", st.Writes, st.Fsyncs, writers*perWriter)
	}
}

func TestReadsDuringWritesAreConsistent(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	const versions = 2000
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := range versions {
			value := fmt.Sprintf("v%05d", i)
			if err := db.Update(Keys("k", "mirror"), func(tx *Tx) error {
				tx.Put("k", []byte(value), 0)
				tx.Put("mirror", []byte(value), 0)
				return nil
			}); err != nil {
				t.Error(err)
				return
			}
		}
	}()
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			last := ""
			for {
				select {
				case <-done:
					return
				default:
				}
				var k, m []byte
				err := db.View(Keys("k", "mirror"), func(tx *Tx) error {
					var err error
					if k, _, err = tx.Get("k"); err != nil {
						return err
					}
					m, _, err = tx.Get("mirror")
					return err
				})
				if err != nil {
					t.Error(err)
					return
				}
				if string(k) != string(m) {
					t.Errorf("torn batch: k=%q mirror=%q", k, m)
					return
				}
				if string(k) < last {
					t.Errorf("value went backwards: %q after %q", k, last)
					return
				}
				last = string(k)
			}
		}()
	}
	wg.Wait()
	expect(t, db, "k", fmt.Sprintf("v%05d", versions-1))
}

func keysInDifferentShards() (string, string) {
	a := "a"
	for i := 0; ; i++ {
		b := fmt.Sprintf("b%d", i)
		if shardIndex(a) != shardIndex(b) {
			return a, b
		}
	}
}

func TestTxRejectsKeysOutsideScope(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	a, b := keysInDifferentShards()
	err := db.Update(Keys(a), func(tx *Tx) error {
		tx.Put(b, []byte("v"), 0)
		return nil
	})
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("put outside scope: err = %v, want ErrNotLocked", err)
	}
	expectMissing(t, db, b)
	err = db.View(Keys(a), func(tx *Tx) error {
		_, _, err := tx.Get(b)
		return err
	})
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("get outside scope: err = %v, want ErrNotLocked", err)
	}
	err = db.View(Keys(a), func(tx *Tx) error {
		tx.Len()
		return nil
	})
	if !errors.Is(err, ErrNotLocked) {
		t.Fatalf("len outside scope: err = %v, want ErrNotLocked", err)
	}
}

func TestParallelWritersAndGlobalReaders(t *testing.T) {
	dir := t.TempDir()
	o := testOptions()
	o.Logs = 4
	db := mustOpen(t, dir, o)
	const writers, perWriter = 8, 300
	var wg sync.WaitGroup
	for g := range writers {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range perWriter {
				a, b := fmt.Sprintf("w%d:%d", g, i), fmt.Sprintf("shared:%d", i%16)
				err := db.Update(Keys(a, b), func(tx *Tx) error {
					v, _, err := tx.Get(b)
					if err != nil {
						return err
					}
					n := 0
					if len(v) > 0 {
						fmt.Sscan(string(v), &n)
					}
					tx.Put(a, []byte("v"), 0)
					tx.Put(b, []byte(fmt.Sprint(n+1)), 0)
					return nil
				})
				if err != nil {
					t.Error(err)
					return
				}
			}
		}()
	}
	stop := make(chan struct{})
	var readers sync.WaitGroup
	readers.Add(1)
	go func() {
		defer readers.Done()
		for {
			select {
			case <-stop:
				return
			default:
			}
			db.Keys(func(k string) bool { return strings.HasPrefix(k, "shared:") })
			db.Len()
			db.Stats()
			if err := db.Merge(); err != nil && !errors.Is(err, ErrMergeInProgress) {
				t.Error(err)
				return
			}
		}
	}()
	wg.Wait()
	close(stop)
	readers.Wait()
	check := func(db *DB) {
		t.Helper()
		if n := db.Len(); n != writers*perWriter+16 {
			t.Fatalf("len = %d, want %d", n, writers*perWriter+16)
		}
		total := 0
		for i := range 16 {
			v, _ := get(t, db, fmt.Sprintf("shared:%d", i))
			n := 0
			fmt.Sscan(v, &n)
			total += n
		}
		if total != writers*perWriter {
			t.Fatalf("shared counters sum to %d, want %d", total, writers*perWriter)
		}
	}
	check(db)
	mustClose(t, db)
	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	check(db)
}

func TestReadOnlyTxRejectsWrites(t *testing.T) {
	db := mustOpen(t, t.TempDir(), testOptions())
	defer mustClose(t, db)
	err := db.View(Keys("k"), func(tx *Tx) error {
		tx.Put("k", []byte("v"), 0)
		return nil
	})
	if !errors.Is(err, ErrReadOnly) {
		t.Fatalf("err = %v, want ErrReadOnly", err)
	}
	expectMissing(t, db, "k")
}

func TestModel(t *testing.T) {
	for _, logs := range []int{1, 4} {
		t.Run(fmt.Sprintf("logs=%d", logs), func(t *testing.T) {
			o := testOptions()
			o.Logs = logs
			runModel(t, o)
		})
	}
}

func runModel(t *testing.T, o Options) {
	dir := t.TempDir()
	db := mustOpen(t, dir, o)
	defer func() { mustClose(t, db) }()
	rng := rand.New(rand.NewPCG(1, 2))
	model := map[string]string{}
	for i := range 20000 {
		key := fmt.Sprintf("key%d", rng.IntN(200))
		switch r := rng.IntN(10); {
		case r < 5:
			value := fmt.Sprintf("value-%d-%s", i, strings.Repeat("x", rng.IntN(40)))
			put(t, db, key, value, 0)
			model[key] = value
		case r < 7:
			_, want := model[key]
			if got := del(t, db, key); got != want {
				t.Fatalf("op %d: delete %q = %v, want %v", i, key, got, want)
			}
			delete(model, key)
		case r < 9:
			got, ok := get(t, db, key)
			want, wantOK := model[key]
			if ok != wantOK || got != want {
				t.Fatalf("op %d: get %q = %q, %v; want %q, %v", i, key, got, ok, want, wantOK)
			}
		default:
			other := fmt.Sprintf("key%d", rng.IntN(200))
			err := db.Update(Keys(key, other), func(tx *Tx) error {
				tx.Put(key, []byte("batch-"+key), 0)
				tx.Delete(other)
				return nil
			})
			if err != nil {
				t.Fatal(err)
			}
			model[key] = "batch-" + key
			delete(model, other)
		}
		if i%2000 == 1999 {
			if err := db.Merge(); err != nil {
				t.Fatal(err)
			}
		}
		if i%5000 == 4999 {
			mustClose(t, db)
			db = mustOpen(t, dir, o)
		}
	}
	for k, v := range model {
		expect(t, db, k, v)
	}
	if n := db.Len(); n != len(model) {
		t.Fatalf("len = %d, want %d", n, len(model))
	}
}
