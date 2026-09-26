package bitcask

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
)

func multiOptions(logs int) Options {
	o := testOptions()
	o.Logs = logs
	return o
}

func keyInLog(logs, log int, prefix string) string {
	for i := 0; ; i++ {
		key := fmt.Sprintf("%s%d", prefix, i)
		if shardIndex(key)%logs == log {
			return key
		}
	}
}

func lastFileOfLog(t *testing.T, dir string, log int) string {
	t.Helper()
	ids, err := listIDs(groupDir(dir, log), dataExt)
	if err != nil || len(ids) == 0 {
		t.Fatalf("log %d has no data files: %v", log, err)
	}
	return filepath.Join(groupDir(dir, log), fileName(ids[len(ids)-1], dataExt))
}

func putPair(t *testing.T, db *DB, a, av, b, bv string) {
	t.Helper()
	err := db.Update(Keys(a, b), func(tx *Tx) error {
		tx.Put(a, []byte(av), 0)
		tx.Put(b, []byte(bv), 0)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestCrossLogBatchCommits(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 0, "a"), keyInLog(4, 1, "b")
	db := mustOpen(t, dir, o)
	putPair(t, db, a, "new", b, "new")
	expect(t, db, a, "new")
	expect(t, db, b, "new")
	mustClose(t, db)

	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	expect(t, db, a, "new")
	expect(t, db, b, "new")
	if st := db.Stats(); st.Logs != 4 || st.Keys != 2 {
		t.Fatalf("stats = %+v", st)
	}
}

func cutTail(t *testing.T, path string, n int64) {
	t.Helper()
	st, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, max(st.Size()-n, 0)); err != nil {
		t.Fatal(err)
	}
}

var commitSize = recordSize(0, 8)

func openCleanup(t *testing.T, dir string, o Options) *DB {
	t.Helper()
	db := mustOpen(t, dir, o)
	t.Cleanup(func() { db.Close() })
	return db
}

func TestCrossLogBatchRollsBackWhenPartIsMissing(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 0, "a"), keyInLog(4, 1, "b")
	db := mustOpen(t, dir, o)
	put(t, db, a, "old", 0)
	put(t, db, b, "old", 0)
	putPair(t, db, a, "new", b, "new")
	mustClose(t, db)

	cutTail(t, lastFileOfLog(t, dir, 0), commitSize)
	cutTail(t, lastFileOfLog(t, dir, 1), commitSize+2)

	db = mustOpen(t, dir, o)
	expect(t, db, a, "old")
	expect(t, db, b, "old")
	if n := db.Len(); n != 2 {
		t.Fatalf("len = %d, want 2", n)
	}
	put(t, db, a, "newer", 0)
	mustClose(t, db)

	db = openCleanup(t, dir, o)
	expect(t, db, a, "newer")
	expect(t, db, b, "old")
	if err := db.Merge(); err != nil {
		t.Fatal(err)
	}
	expect(t, db, a, "newer")
	expect(t, db, b, "old")
}

func TestCrossLogRollbackRemovesCreatedKeys(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 2, "x"), keyInLog(4, 3, "y")
	db := mustOpen(t, dir, o)
	putPair(t, db, a, "1", b, "1")
	mustClose(t, db)

	cutTail(t, lastFileOfLog(t, dir, 2), commitSize)
	if err := os.Truncate(lastFileOfLog(t, dir, 3), 5); err != nil {
		t.Fatal(err)
	}

	db = openCleanup(t, dir, o)
	expectMissing(t, db, a)
	expectMissing(t, db, b)
	if n := db.Len(); n != 0 {
		t.Fatalf("len = %d, want 0", n)
	}
}

func TestCrossLogCommitInOneLogIsEnoughAndRepaired(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	a, b := keyInLog(4, 0, "a"), keyInLog(4, 1, "b")
	db := mustOpen(t, dir, o)
	putPair(t, db, a, "new", b, "new")
	mustClose(t, db)

	cutTail(t, lastFileOfLog(t, dir, 1), commitSize)

	db = mustOpen(t, dir, o)
	expect(t, db, a, "new")
	expect(t, db, b, "new")
	if err := db.groups[0].merge(); err != nil {
		t.Fatal(err)
	}
	mustClose(t, db)

	db = openCleanup(t, dir, o)
	expect(t, db, a, "new")
	expect(t, db, b, "new")
}

func TestLogCountIsPersisted(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, multiOptions(4))
	put(t, db, "k", "v", 0)
	mustClose(t, db)

	db = mustOpen(t, dir, multiOptions(0))
	if st := db.Stats(); st.Logs != 4 {
		t.Fatalf("logs = %d, want 4", st.Logs)
	}
	expect(t, db, "k", "v")
	mustClose(t, db)

	if _, err := Open(dir, multiOptions(2)); !errors.Is(err, ErrLayout) {
		t.Fatalf("open with a different log count: err = %v, want ErrLayout", err)
	}
}

func TestLegacySingleLogLayoutOpens(t *testing.T) {
	dir := t.TempDir()
	db := mustOpen(t, dir, multiOptions(1))
	put(t, db, "a", "1", 0)
	put(t, db, "b", "2", 0)
	mustClose(t, db)
	entries, err := os.ReadDir(logDir(dir))
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if err := os.Rename(filepath.Join(logDir(dir), e.Name()), filepath.Join(dir, e.Name())); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Remove(logDir(dir)); err != nil {
		t.Fatal(err)
	}
	if err := os.Remove(filepath.Join(dir, metaName)); err != nil {
		t.Fatal(err)
	}

	if _, err := Open(dir, multiOptions(4)); !errors.Is(err, ErrLayout) {
		t.Fatalf("open legacy layout with 4 logs: err = %v, want ErrLayout", err)
	}
	db = mustOpen(t, dir, multiOptions(0))
	defer mustClose(t, db)
	if st := db.Stats(); st.Logs != 1 {
		t.Fatalf("logs = %d, want 1", st.Logs)
	}
	expect(t, db, "a", "1")
	expect(t, db, "b", "2")
	put(t, db, "c", "3", 0)
	expect(t, db, "c", "3")
	if fileExists(filepath.Join(dir, metaName)) || fileExists(logDir(dir)) {
		t.Fatal("legacy database was converted to the multi-log layout")
	}
}

func TestLogsWriteInParallel(t *testing.T) {
	dir := t.TempDir()
	o := multiOptions(4)
	db := mustOpen(t, dir, o)
	for i := range 400 {
		put(t, db, fmt.Sprintf("k%d", i), "v", 0)
	}
	used := 0
	for i := range 4 {
		if ids, _ := listIDs(groupDir(dir, i), dataExt); len(ids) > 0 {
			if fi, err := os.Stat(filepath.Join(groupDir(dir, i), fileName(ids[len(ids)-1], dataExt))); err == nil && fi.Size() > 0 {
				used++
			}
		}
	}
	if used != 4 {
		t.Fatalf("only %d of 4 logs received writes", used)
	}
	mustClose(t, db)
	db = mustOpen(t, dir, o)
	defer mustClose(t, db)
	if n := db.Len(); n != 400 {
		t.Fatalf("len after reopen = %d, want 400", n)
	}
}
