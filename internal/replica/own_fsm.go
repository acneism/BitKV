package replica

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/gliedabrennung/raft"
	"github.com/gliedabrennung/raft/node"
)

const (
	durableName  = "fsm-durable"
	snapshotInfo = "bitkv-snapshot"
)

type bitcaskFSM struct {
	db      *bitcask.DB
	dir     string
	applied atomic.Uint64
	durable atomic.Uint64
}

func newBitcaskFSM(db *bitcask.DB, dir string) (*bitcaskFSM, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	f := &bitcaskFSM{db: db, dir: dir}
	b, err := os.ReadFile(filepath.Join(dir, durableName))
	switch {
	case errors.Is(err, fs.ErrNotExist):
	case err != nil:
		return nil, err
	case len(b) == 12 && crc32.ChecksumIEEE(b[:8]) == binary.LittleEndian.Uint32(b[8:]):
		if db.Len() > 0 {
			f.durable.Store(binary.LittleEndian.Uint64(b))
		}
	default:
		return nil, errors.New("replica: corrupt " + durableName)
	}
	f.applied.Store(f.durable.Load())
	return f, nil
}

func (f *bitcaskFSM) setDurable(index uint64) error {
	b := binary.LittleEndian.AppendUint64(nil, index)
	b = binary.LittleEndian.AppendUint32(b, crc32.ChecksumIEEE(b))
	tmp := filepath.Join(f.dir, durableName+".tmp")
	if err := writeSync(tmp, b); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(f.dir, durableName)); err != nil {
		return err
	}
	if err := syncDir(f.dir); err != nil {
		return err
	}
	f.durable.Store(index)
	return nil
}

func writeSync(path string, b []byte) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	_, err = file.Write(b)
	if err == nil {
		err = file.Sync()
	}
	if cerr := file.Close(); err == nil {
		err = cerr
	}
	return err
}

func encodeOwnEntry(kind byte, ops []bitcask.Op) []byte {
	size := 1
	for _, op := range ops {
		size += 1 + 3*binary.MaxVarintLen64 + len(op.Key) + len(op.Value)
	}
	b := append(make([]byte, 0, size), kind)
	for _, op := range ops {
		b = appendOp(b, op)
	}
	return b
}

func (f *bitcaskFSM) Apply(ents []raft.Entry) error {
	var ops []bitcask.Op
	var upTo uint64
	flush := func() error {
		if len(ops) == 0 {
			return nil
		}
		err := f.db.Apply(ops, upTo)
		ops, upTo = ops[:0], 0
		return err
	}
	for _, e := range ents {
		if e.Type != raft.EntryNormal {
			continue
		}
		if len(e.Data) == 0 {
			return errEntry
		}
		switch e.Data[0] {
		case kindOps:
			decoded, err := decodeOps(bytes.NewReader(e.Data[1:]))
			if err != nil {
				return err
			}
			ops = append(ops, decoded...)
			upTo = e.Index
		case kindFlush:
			if err := flush(); err != nil {
				return err
			}
			if err := f.db.Flush(); err != nil {
				return err
			}
		default:
			return errEntry
		}
	}
	if err := flush(); err != nil {
		return err
	}
	if n := len(ents); n > 0 {
		f.applied.Store(ents[n-1].Index)
	}
	return nil
}

func (f *bitcaskFSM) DurableIndex() uint64 { return f.durable.Load() }

func (f *bitcaskFSM) Snapshot(dir string) (raft.SnapshotMeta, error) {
	index := f.applied.Load()
	logs, files, err := f.db.LinkFiles(dir)
	if err != nil {
		return raft.SnapshotMeta{}, err
	}
	info := appendFileList(nil, logs, len(files))
	for _, sf := range files {
		info = appendFileEntry(info, sf)
	}
	if err := writeSync(filepath.Join(dir, snapshotInfo), info); err != nil {
		return raft.SnapshotMeta{}, err
	}
	if err := f.setDurable(index); err != nil {
		return raft.SnapshotMeta{}, err
	}
	return raft.SnapshotMeta{Index: index}, nil
}

func (f *bitcaskFSM) Restore(src node.SnapshotSource) error {
	info, err := os.ReadFile(filepath.Join(src.Dir, snapshotInfo))
	if err != nil {
		return err
	}
	logs, files, err := readFileList(bufio.NewReader(bytes.NewReader(info)), nil)
	if err != nil {
		return err
	}
	tmp := filepath.Join(f.dir, restoreDir)
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for _, sf := range files {
		if err := copyPrefix(filepath.Join(src.Dir, filepath.FromSlash(sf.Path)), filepath.Join(tmp, filepath.FromSlash(sf.Path)), sf.Size); err != nil {
			return err
		}
	}
	opts := bitcask.DefaultOptions()
	opts.Logs = logs
	opts.Sync = bitcask.SyncNo
	opts.MergeInterval = 0
	opts.ExpireInterval = 0
	opts.Now = f.db.Options().Now
	from, err := bitcask.Open(tmp, opts)
	if err != nil {
		return err
	}
	defer from.Close()
	if err := f.db.Flush(); err != nil {
		return err
	}
	batch := make([]bitcask.Op, 0, restoreBatch)
	err = from.Dump(func(op bitcask.Op) error {
		if batch = append(batch, op); len(batch) < restoreBatch {
			return nil
		}
		err := f.db.Apply(batch, 0)
		batch = batch[:0]
		return err
	})
	if err == nil {
		err = f.db.Apply(batch, 0)
	}
	if err == nil {
		err = f.db.Sync()
	}
	if err != nil {
		return err
	}
	f.applied.Store(src.Meta.Index)
	return f.setDurable(src.Meta.Index)
}

func copyPrefix(from, to string, size int64) error {
	if err := os.MkdirAll(filepath.Dir(to), 0o755); err != nil {
		return err
	}
	in, err := os.Open(from)
	if err != nil {
		return err
	}
	defer in.Close()
	out, err := os.Create(to)
	if err != nil {
		return err
	}
	_, err = io.CopyN(out, in, size)
	if cerr := out.Close(); err == nil {
		err = cerr
	}
	return err
}
