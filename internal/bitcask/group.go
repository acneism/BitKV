package bitcask

import (
	"bytes"
	"fmt"
	"slices"
	"sync"
	"sync/atomic"
)

type logGroup struct {
	db  *DB
	id  int
	dir string

	logMu  sync.Mutex
	active *dataFile
	seq    uint64

	filesMu sync.RWMutex
	files   map[uint32]*dataFile

	qMu   sync.Mutex
	queue []*pendingBatch

	wmu     sync.Mutex
	wcond   *sync.Cond
	writing bool
	written uint64
	scratch []byte

	syncMu   sync.Mutex
	syncCond *sync.Cond
	syncing  bool
	synced   uint64

	mergeMu    sync.Mutex
	totalBytes atomic.Int64
}

func newLogGroup(db *DB, id int, dir string) *logGroup {
	g := &logGroup{db: db, id: id, dir: dir, files: make(map[uint32]*dataFile)}
	g.wcond = sync.NewCond(&g.wmu)
	g.syncCond = sync.NewCond(&g.syncMu)
	return g
}

func (g *logGroup) reserve(b *pendingBatch) error {
	g.logMu.Lock()
	defer g.logMu.Unlock()
	if err := g.db.stateErr(); err != nil {
		return err
	}
	return g.reserveLocked(b)
}

func (g *logGroup) reserveLocked(b *pendingBatch) error {
	n := int64(len(b.buf))
	if g.active.size > 0 && g.active.size+n > g.db.opts.MaxFileSize {
		if err := g.rotateLocked(g.active.id + 1); err != nil {
			return err
		}
	}
	b.df = g.active
	b.off = b.df.size
	b.df.size += n
	g.totalBytes.Add(n)
	g.seq++
	b.seq = g.seq
	g.qMu.Lock()
	g.queue = append(g.queue, b)
	g.qMu.Unlock()
	return nil
}

func (g *logGroup) rotateLocked(id uint32) error {
	if err := g.waitWritten(g.seq); err != nil {
		return err
	}
	if err := g.active.f.Sync(); err != nil {
		return g.db.fail(err)
	}
	g.db.fsyncs.Add(1)
	return g.newActive(id)
}

func (g *logGroup) newActive(id uint32) error {
	df, err := openDataFile(g.dir, id)
	if err != nil {
		return err
	}
	if err := syncDir(g.dir); err != nil {
		df.close()
		return err
	}
	g.filesMu.Lock()
	g.files[id] = df
	g.filesMu.Unlock()
	g.active = df
	g.totalBytes.Add(df.size)
	return nil
}

func (g *logGroup) dropFileLocked(id uint32) {
	df, ok := g.files[id]
	if !ok {
		return
	}
	df.close()
	g.totalBytes.Add(-df.size)
	delete(g.files, id)
}

func (g *logGroup) fileIDs() []uint32 {
	g.filesMu.RLock()
	defer g.filesMu.RUnlock()
	ids := make([]uint32, 0, len(g.files))
	for id := range g.files {
		ids = append(ids, id)
	}
	slices.Sort(ids)
	return ids
}

func (g *logGroup) dataFiles() int {
	g.filesMu.RLock()
	defer g.filesMu.RUnlock()
	return len(g.files)
}

func (g *logGroup) closeFiles() error {
	g.filesMu.Lock()
	defer g.filesMu.Unlock()
	var err error
	for _, df := range g.files {
		if cerr := df.close(); cerr != nil && err == nil {
			err = cerr
		}
	}
	return err
}

func (g *logGroup) waitWritten(seq uint64) error {
	g.wmu.Lock()
	defer g.wmu.Unlock()
	for g.written < seq {
		if err := g.db.failure(); err != nil {
			return err
		}
		if g.writing {
			g.wcond.Wait()
			continue
		}
		g.qMu.Lock()
		batches := g.queue
		g.queue = nil
		g.qMu.Unlock()
		g.writing = true
		g.wmu.Unlock()
		err := g.writeBatches(batches)
		g.wmu.Lock()
		g.writing = false
		if err != nil {
			g.db.fail(err)
		} else if len(batches) > 0 {
			g.written = batches[len(batches)-1].seq
		}
		g.wcond.Broadcast()
	}
	return nil
}

func (g *logGroup) writeBatches(batches []*pendingBatch) error {
	for i := 0; i < len(batches); {
		first := batches[i]
		end := first.end()
		j := i + 1
		for j < len(batches) && batches[j].df == first.df && batches[j].off == end {
			end = batches[j].end()
			j++
		}
		buf := first.buf
		if j > i+1 {
			g.scratch = g.scratch[:0]
			for _, b := range batches[i:j] {
				g.scratch = append(g.scratch, b.buf...)
			}
			buf = g.scratch
		}
		if err := writeFull(first.df.f, buf, first.off); err != nil {
			return err
		}
		g.db.writes.Add(1)
		first.df.written.Store(end)
		i = j
	}
	if cap(g.scratch) > maxScratch {
		g.scratch = nil
	}
	for _, b := range batches {
		for _, r := range b.refs {
			g.db.kd.shard(r.key).dropOverlay(r.key, b.df.id, b.off+r.offset)
		}
	}
	return nil
}

func (g *logGroup) readEntry(s *shard, key string, e entry) ([]byte, error) {
	g.filesMu.RLock()
	defer g.filesMu.RUnlock()
	df, ok := g.files[e.fileID]
	if !ok {
		return nil, fmt.Errorf("%w: log %d has no data file %d", ErrCorrupt, g.id, e.fileID)
	}
	if e.offset+recordSize(len(key), int(e.valueSize)) > df.written.Load() {
		if v, ok := s.getOverlay(key, e.fileID, e.offset); ok {
			return bytes.Clone(v), nil
		}
	}
	return df.read(e.offset, key, e.valueSize)
}

func (g *logGroup) sync() error {
	g.logMu.Lock()
	seq := g.seq
	g.logMu.Unlock()
	if err := g.waitWritten(seq); err != nil {
		return err
	}
	return g.waitSync(seq)
}

func (g *logGroup) waitSync(seq uint64) error {
	g.syncMu.Lock()
	defer g.syncMu.Unlock()
	for g.synced < seq {
		if g.syncing {
			g.syncCond.Wait()
			continue
		}
		g.syncing = true
		g.syncMu.Unlock()
		target, err := g.syncOnce()
		g.syncMu.Lock()
		g.syncing = false
		if err == nil && target > g.synced {
			g.synced = target
		}
		g.syncCond.Broadcast()
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *logGroup) syncOnce() (uint64, error) {
	g.wmu.Lock()
	target := g.written
	g.wmu.Unlock()
	if err := g.db.failure(); err != nil {
		return 0, err
	}
	g.logMu.Lock()
	closed, df := g.db.closed.Load(), g.active
	g.logMu.Unlock()
	if closed {
		return target, nil
	}
	err := df.f.Sync()
	if err == nil {
		g.db.fsyncs.Add(1)
		return target, nil
	}
	g.filesMu.RLock()
	retired := g.files[df.id] != df
	g.filesMu.RUnlock()
	if retired || g.db.closed.Load() {
		return target, nil
	}
	return 0, g.db.fail(err)
}
