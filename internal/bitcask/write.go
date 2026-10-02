package bitcask

import (
	"os"
	"time"
)

const maxScratch = 8 << 20

type overlayEntry struct {
	fileID uint32
	kind   Kind
	offset int64
	value  []byte
}

type overlayRef struct {
	key    string
	offset int64
}

type pendingBatch struct {
	df   *dataFile
	off  int64
	buf  []byte
	seq  uint64
	refs []overlayRef
}

func (b *pendingBatch) end() int64 {
	return b.off + int64(len(b.buf))
}

type waitPoint struct {
	g   *logGroup
	seq uint64
}

func (db *DB) await(waits []waitPoint) error {
	for _, w := range waits {
		if err := w.g.waitWritten(w.seq); err != nil {
			return err
		}
	}
	if db.policy() != SyncAlways {
		return nil
	}
	for _, w := range waits {
		if err := w.g.waitSync(w.seq); err != nil {
			return err
		}
	}
	return nil
}

func (db *DB) fail(err error) error {
	db.failed.CompareAndSwap(nil, &err)
	return *db.failed.Load()
}

func (db *DB) failure() error {
	if p := db.failed.Load(); p != nil {
		return *p
	}
	return nil
}

func (db *DB) stateErr() error {
	if db.closed.Load() {
		return ErrClosed
	}
	return db.failure()
}

func (db *DB) fsync(f *os.File) error {
	start := time.Now()
	err := f.Sync()
	db.fsyncTime.Add(int64(time.Since(start)))
	if err == nil {
		db.fsyncs.Add(1)
	}
	return err
}

func (db *DB) Sync() error {
	if err := db.reserveMarks(); err != nil {
		return err
	}
	for _, g := range db.groups {
		if err := g.sync(); err != nil {
			return err
		}
	}
	return nil
}
