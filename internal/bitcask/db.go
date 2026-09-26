package bitcask

import (
	"crypto/rand"
	"encoding/binary"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"time"
)

var (
	ErrCorrupt         = errors.New("bitcask: corrupted data")
	ErrLocked          = errors.New("bitcask: database is locked by another process")
	ErrClosed          = errors.New("bitcask: database is closed")
	ErrMergeInProgress = errors.New("bitcask: merge already in progress")
	ErrReadOnly        = errors.New("bitcask: write in read-only transaction")
	ErrTooLarge        = errors.New("bitcask: key or value too large")
)

type Stats struct {
	Keys        int
	KeysWithTTL int
	Logs        int
	DataFiles   int
	TotalBytes  int64
	LiveBytes   int64
	Merges      int64
	Writes      int64
	Fsyncs      int64
	ExpiredKeys int64
}

type DB struct {
	dir    string
	opts   Options
	lock   *fileLock
	kd     *keydir
	groups []*logGroup

	txGate sync.RWMutex
	txBase uint64
	txSeq  atomic.Uint64

	failed      atomic.Pointer[error]
	closed      atomic.Bool
	merges      atomic.Int64
	writes      atomic.Int64
	fsyncs      atomic.Int64
	expiredKeys atomic.Int64

	expireCursor int

	stop     chan struct{}
	stopOnce sync.Once
	wg       sync.WaitGroup
}

func (db *DB) nowMs() int64 {
	return db.opts.Now().UnixMilli()
}

func Open(dir string, opts Options) (*DB, error) {
	if opts.MaxFileSize <= 0 {
		opts.MaxFileSize = DefaultOptions().MaxFileSize
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	lock, err := lockFile(filepath.Join(dir, lockName))
	if err != nil {
		return nil, err
	}
	db := &DB{dir: dir, opts: opts, lock: lock, kd: newKeydir(), stop: make(chan struct{})}
	if err := db.open(); err != nil {
		for _, g := range db.groups {
			g.closeFiles()
		}
		lock.unlock()
		return nil, err
	}
	db.startBackground()
	return db, nil
}

func (db *DB) open() error {
	dirs, err := db.layout()
	if err != nil {
		return err
	}
	for i, d := range dirs {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return err
		}
		if err := recoverMerge(d); err != nil {
			return err
		}
		db.groups = append(db.groups, newLogGroup(db, i, d))
	}
	if err := recoverFlush(db.dir, dirs); err != nil {
		return err
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return err
	}
	db.txBase = binary.LittleEndian.Uint64(seed[:])
	return db.load()
}

func (db *DB) groupOfShard(i int) *logGroup {
	return db.groups[i%len(db.groups)]
}

func (db *DB) groupOfKey(key string) *logGroup {
	return db.groupOfShard(shardIndex(key))
}

func (db *DB) nextTxID() uint64 {
	return db.txBase + db.txSeq.Add(1)
}

func (db *DB) lockAll() {
	for i := range db.kd.shards {
		db.kd.shards[i].mu.Lock()
	}
}

func (db *DB) unlockAll() {
	for i := range db.kd.shards {
		db.kd.shards[i].mu.Unlock()
	}
}

func (db *DB) lockGroups() {
	for _, g := range db.groups {
		g.mergeMu.Lock()
	}
	db.lockAll()
	for _, g := range db.groups {
		g.logMu.Lock()
	}
}

func (db *DB) unlockGroups() {
	for _, g := range db.groups {
		g.logMu.Unlock()
	}
	db.unlockAll()
	for _, g := range db.groups {
		g.mergeMu.Unlock()
	}
}

func (db *DB) Close() error {
	db.stopOnce.Do(func() { close(db.stop) })
	db.wg.Wait()
	db.lockGroups()
	defer db.unlockGroups()
	if db.closed.Load() {
		return nil
	}
	db.closed.Store(true)
	var err error
	if db.failure() == nil {
		for _, g := range db.groups {
			if err = g.waitWritten(g.seq); err != nil {
				break
			}
			if err = g.active.f.Sync(); err != nil {
				db.fail(err)
				break
			}
		}
	}
	for _, g := range db.groups {
		if cerr := g.closeFiles(); cerr != nil && err == nil {
			err = cerr
		}
	}
	if lerr := db.lock.unlock(); lerr != nil && err == nil {
		err = lerr
	}
	return err
}

func (db *DB) Options() Options {
	return db.opts
}

func (db *DB) Len() int {
	n, _, _ := db.kd.totals()
	return n
}

func (db *DB) Stats() Stats {
	keys, ttl, live := db.kd.totals()
	st := Stats{
		Keys:        keys,
		KeysWithTTL: ttl,
		Logs:        len(db.groups),
		LiveBytes:   live,
		Merges:      db.merges.Load(),
		Writes:      db.writes.Load(),
		Fsyncs:      db.fsyncs.Load(),
		ExpiredKeys: db.expiredKeys.Load(),
	}
	for _, g := range db.groups {
		st.DataFiles += g.dataFiles()
		st.TotalBytes += g.totalBytes.Load()
	}
	return st
}

func (db *DB) Scan(cursor uint64, count int, match func(string) bool) (uint64, []string) {
	var next uint64
	var keys []string
	_ = db.View(Shardwise(), func(tx *Tx) error {
		next, keys = tx.Scan(cursor, count, match)
		return nil
	})
	return next, keys
}

func (db *DB) Keys(match func(string) bool) []string {
	_, keys := db.Scan(0, math.MaxInt, match)
	return keys
}

func (db *DB) Flush() error {
	db.lockGroups()
	defer db.unlockGroups()
	if err := db.stateErr(); err != nil {
		return err
	}
	bounds := make([]uint32, len(db.groups))
	old := make([][]uint32, len(db.groups))
	for i, g := range db.groups {
		old[i] = g.fileIDs()
		bounds[i] = g.active.id
		if err := g.rotateLocked(g.active.id + 1); err != nil {
			return err
		}
	}
	if err := writeFlushMarker(db.dir, bounds); err != nil {
		return err
	}
	db.kd.reset()
	removed := true
	for i, g := range db.groups {
		g.filesMu.Lock()
		for _, id := range old[i] {
			g.dropFileLocked(id)
		}
		g.filesMu.Unlock()
		for _, id := range old[i] {
			if removeFiles(g.dir, id) != nil {
				removed = false
			}
		}
	}
	if removed {
		if err := os.Remove(filepath.Join(db.dir, flushName)); err == nil {
			_ = syncDir(db.dir)
		}
	}
	return nil
}

func (db *DB) Merge() error {
	busy := false
	for _, g := range db.groups {
		err := g.merge()
		if errors.Is(err, ErrMergeInProgress) {
			busy = true
			continue
		}
		if err != nil {
			return err
		}
	}
	if busy {
		return ErrMergeInProgress
	}
	return nil
}

func (db *DB) startBackground() {
	if db.opts.Sync == SyncEverySec {
		db.every(time.Second, func() { _ = db.Sync() })
	}
	if db.opts.ExpireInterval > 0 {
		db.every(db.opts.ExpireInterval, db.expireCycle)
	}
	if db.opts.MergeInterval > 0 {
		db.every(db.opts.MergeInterval, db.maybeMerge)
	}
}

func (db *DB) every(interval time.Duration, fn func()) {
	db.wg.Add(1)
	go func() {
		defer db.wg.Done()
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-db.stop:
				return
			case <-t.C:
				fn()
			}
		}
	}()
}

func (db *DB) maybeMerge() {
	live := make([]int64, len(db.groups))
	for i := range db.kd.shards {
		s := &db.kd.shards[i]
		s.mu.RLock()
		live[i%len(db.groups)] += s.live
		s.mu.RUnlock()
	}
	minBytes := db.opts.MergeMinBytes / int64(len(db.groups))
	for i, g := range db.groups {
		total := g.totalBytes.Load()
		if total == 0 || total < minBytes || total <= live[i] {
			continue
		}
		if float64(total-live[i])/float64(total) < db.opts.MergeRatio {
			continue
		}
		_ = g.merge()
	}
}
