package bitcask

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

type relocation struct {
	key       string
	oldFile   uint32
	oldOffset int64
	newFile   uint32
	newOffset int64
}

type mergeResult struct {
	moved   []relocation
	expired []relocation
	files   []uint32
}

func (g *logGroup) merge() error {
	if !g.mergeMu.TryLock() {
		return ErrMergeInProgress
	}
	defer g.mergeMu.Unlock()
	if err := g.db.stateErr(); err != nil {
		return err
	}
	if err := recoverMerge(g.dir); err != nil {
		return err
	}
	inputs, boundary, reserve, err := g.beginMerge()
	if err != nil {
		return err
	}
	g.logMu.Lock()
	err = g.carryMarkLocked()
	g.logMu.Unlock()
	if err != nil {
		return err
	}
	mergeDir := filepath.Join(g.dir, mergeDirName)
	res, err := g.writeMerge(mergeDir, inputs, boundary, reserve)
	if err != nil {
		_ = os.RemoveAll(mergeDir)
		return err
	}
	return g.finishMerge(mergeDir, boundary, res)
}

func (g *logGroup) beginMerge() ([]*dataFile, uint32, uint32, error) {
	g.db.txGate.Lock()
	defer g.db.txGate.Unlock()
	g.logMu.Lock()
	defer g.logMu.Unlock()
	if err := g.db.stateErr(); err != nil {
		return nil, 0, 0, err
	}
	g.filesMu.RLock()
	var inputs []*dataFile
	for _, id := range g.sortedIDsLocked() {
		inputs = append(inputs, g.files[id])
	}
	g.filesMu.RUnlock()
	boundary := g.active.id
	reserve := uint32(len(inputs))
	if err := g.rotateLocked(boundary + reserve + 1); err != nil {
		return nil, 0, 0, err
	}
	return inputs, boundary, reserve, nil
}

func (g *logGroup) writeMerge(dir string, inputs []*dataFile, boundary, reserve uint32) (*mergeResult, error) {
	if err := os.RemoveAll(dir); err != nil {
		return nil, err
	}
	if err := os.MkdirAll(dir, dirMode); err != nil {
		return nil, err
	}
	w := &mergeWriter{dir: dir, nextID: boundary + 1, lastID: boundary + reserve, maxSize: g.db.opts.MaxFileSize}
	defer w.close()
	res := &mergeResult{}
	scanned := 0
	for _, df := range inputs {
		now := g.db.nowMs()
		sc := newScanner(df.f, df.size)
		for {
			rec, err := sc.next()
			if err == io.EOF {
				break
			}
			if err != nil {
				return nil, fmt.Errorf("%w: %s: %v", ErrCorrupt, filepath.Join(g.dir, fileName(df.id, dataExt)), err)
			}
			scanned++
			if scanned%1024 == 0 {
				select {
				case <-g.db.stop:
					return nil, ErrClosed
				default:
				}
			}
			if rec.flags&(flagTombstone|flagFlush|flagTx|flagTxCommit|flagMark) != 0 {
				continue
			}
			s := &g.db.kd.shards[shardIndex(rec.key)]
			s.mu.RLock()
			e, ok := s.m[string(rec.key)]
			s.mu.RUnlock()
			if !ok || e.fileID != df.id || e.offset != rec.offset {
				continue
			}
			key := string(rec.key)
			if e.expired(now) {
				res.expired = append(res.expired, relocation{key: key, oldFile: df.id, oldOffset: rec.offset})
				continue
			}
			id, off, err := w.write(key, rec.value, e.expireAt)
			if err != nil {
				return nil, err
			}
			res.moved = append(res.moved, relocation{key: key, oldFile: df.id, oldOffset: rec.offset, newFile: id, newOffset: off})
		}
	}
	if err := w.finish(); err != nil {
		return nil, err
	}
	res.files = w.ids
	if err := writeMarker(dir, boundary); err != nil {
		return nil, err
	}
	return res, nil
}

func (g *logGroup) finishMerge(mergeDir string, boundary uint32, res *mergeResult) error {
	for _, ext := range []string{dataExt, hintExt} {
		for _, id := range res.files {
			name := fileName(id, ext)
			if err := os.Rename(filepath.Join(mergeDir, name), filepath.Join(g.dir, name)); err != nil {
				return g.failMerge(err)
			}
		}
	}
	if err := syncDir(g.dir); err != nil {
		return g.failMerge(err)
	}
	opened := make([]*dataFile, 0, len(res.files))
	for _, id := range res.files {
		df, err := openDataFile(g.dir, id)
		if err != nil {
			for _, o := range opened {
				o.close()
			}
			return g.failMerge(err)
		}
		df.seal()
		opened = append(opened, df)
	}
	g.filesMu.Lock()
	for _, df := range opened {
		g.files[df.id] = df
		g.totalBytes.Add(df.size)
	}
	g.filesMu.Unlock()
	for _, m := range res.moved {
		s := g.db.kd.shard(m.key)
		s.mu.Lock()
		s.relocate(m.key, m.oldFile, m.oldOffset, m.newFile, m.newOffset)
		s.mu.Unlock()
	}
	for _, m := range res.expired {
		s := g.db.kd.shard(m.key)
		s.mu.Lock()
		s.removeAt(m.key, m.oldFile, m.oldOffset)
		s.mu.Unlock()
	}
	var old []uint32
	g.filesMu.Lock()
	for id := range g.files {
		if id <= boundary {
			old = append(old, id)
			g.dropFileLocked(id)
		}
	}
	g.filesMu.Unlock()
	g.db.merges.Add(1)
	var rmErr error
	for _, id := range old {
		rmErr = errors.Join(rmErr, removeFiles(g.dir, id))
	}
	if rmErr != nil {
		return nil
	}
	_ = syncDir(g.dir)
	return os.RemoveAll(mergeDir)
}

func (g *logGroup) failMerge(err error) error {
	return g.db.fail(fmt.Errorf("bitcask: merge swap failed in log %d, restart required: %w", g.id, err))
}

type mergeWriter struct {
	dir     string
	nextID  uint32
	lastID  uint32
	maxSize int64
	id      uint32
	size    int64
	data    *os.File
	hint    *os.File
	dw      *bufio.Writer
	hw      *bufio.Writer
	buf     []byte
	ids     []uint32
}

func (w *mergeWriter) write(key string, value []byte, expireAt int64) (uint32, int64, error) {
	n := recordSize(len(key), len(value))
	if w.data == nil || (w.size > 0 && w.size+n > w.maxSize && w.nextID <= w.lastID) {
		if err := w.rotate(); err != nil {
			return 0, 0, err
		}
	}
	off := w.size
	w.buf = appendRecord(w.buf[:0], 0, expireAt, key, value)
	if _, err := w.dw.Write(w.buf); err != nil {
		return 0, 0, err
	}
	w.buf = appendHint(w.buf[:0], expireAt, off, key, uint32(len(value)))
	if _, err := w.hw.Write(w.buf); err != nil {
		return 0, 0, err
	}
	w.size += n
	return w.id, off, nil
}

func (w *mergeWriter) rotate() error {
	if err := w.finish(); err != nil {
		return err
	}
	id := w.nextID
	w.nextID++
	data, err := os.OpenFile(filepath.Join(w.dir, fileName(id, dataExt)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return err
	}
	hint, err := os.OpenFile(filepath.Join(w.dir, fileName(id, hintExt)), os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		data.Close()
		return err
	}
	w.id, w.size, w.data, w.hint = id, 0, data, hint
	w.dw = bufio.NewWriterSize(data, 1<<20)
	w.hw = bufio.NewWriterSize(hint, 256<<10)
	w.ids = append(w.ids, id)
	return nil
}

func (w *mergeWriter) finish() error {
	if w.data == nil {
		return nil
	}
	err := errors.Join(w.dw.Flush(), w.hw.Flush(), w.data.Sync(), w.hint.Sync())
	w.close()
	return err
}

func (w *mergeWriter) close() {
	if w.data == nil {
		return
	}
	w.data.Close()
	w.hint.Close()
	w.data, w.hint = nil, nil
}
