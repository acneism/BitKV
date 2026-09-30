package replica

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

const (
	kindOps    byte = 1
	kindFlush  byte = 2
	flagDelete byte = 1

	maxField     = 1<<32 - 1
	restoreBatch = 1024
	restoreDir   = "restore"
	snapshotInfo = "casketdb-snapshot"
)

var (
	errEntry    = errors.New("replica: malformed log entry")
	errSnapshot = errors.New("replica: malformed snapshot")
)

type bitcaskFSM struct {
	db      *bitcask.DB
	dir     string
	applied atomic.Uint64
	first   atomic.Uint64
}

func newBitcaskFSM(db *bitcask.DB, dir string) (*bitcaskFSM, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	f := &bitcaskFSM{db: db, dir: dir}
	f.applied.Store(db.DurableIndex())
	return f, nil
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
		f.first.CompareAndSwap(0, ents[0].Index)
		f.db.MarkApplied(ents[n-1].Index)
		f.applied.Store(ents[n-1].Index)
	}
	return nil
}

func (f *bitcaskFSM) DurableIndex() uint64 { return f.db.DurableIndex() }

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
	return raft.SnapshotMeta{Index: index}, nil
}

func (f *bitcaskFSM) Restore(src node.SnapshotSource) error {
	info, err := os.ReadFile(filepath.Join(src.Dir, snapshotInfo))
	if err != nil {
		return err
	}
	logs, files, err := readFileList(bufio.NewReader(bytes.NewReader(info)))
	if err != nil {
		return err
	}
	tmp := filepath.Join(f.dir, restoreDir)
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	for _, sf := range files {
		if err := bitcask.CopyPrefix(filepath.Join(src.Dir, filepath.FromSlash(sf.Path)), filepath.Join(tmp, filepath.FromSlash(sf.Path)), sf.Size); err != nil {
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
		f.db.MarkApplied(src.Meta.Index)
		err = f.db.Sync()
	}
	if err != nil {
		return err
	}
	f.applied.Store(src.Meta.Index)
	return nil
}

func encodeEntry(kind byte, ops []bitcask.Op) []byte {
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

func decodeOps(body *bytes.Reader) ([]bitcask.Op, error) {
	var ops []bitcask.Op
	for body.Len() > 0 {
		op, err := readOp(body)
		if err != nil {
			return nil, errEntry
		}
		ops = append(ops, op)
	}
	return ops, nil
}

func appendOp(b []byte, op bitcask.Op) []byte {
	var flags byte
	if op.Delete {
		flags = flagDelete
	}
	b = append(b, flags)
	b = binary.AppendVarint(b, op.ExpireAt)
	b = binary.AppendUvarint(b, uint64(len(op.Key)))
	b = append(b, op.Key...)
	b = binary.AppendUvarint(b, uint64(len(op.Value)))
	return append(b, op.Value...)
}

type byteReader interface {
	io.Reader
	io.ByteReader
}

func readOp(r byteReader) (bitcask.Op, error) {
	flags, err := r.ReadByte()
	if err != nil {
		return bitcask.Op{}, err
	}
	op := bitcask.Op{Delete: flags&flagDelete != 0}
	if op.ExpireAt, err = binary.ReadVarint(r); err != nil {
		return op, unexpected(err)
	}
	key, err := readField(r)
	if err != nil {
		return op, err
	}
	op.Key = string(key)
	op.Value, err = readField(r)
	return op, err
}

func readField(r byteReader) ([]byte, error) {
	n, err := binary.ReadUvarint(r)
	if err != nil {
		return nil, unexpected(err)
	}
	if n > maxField {
		return nil, errEntry
	}
	b := make([]byte, n)
	_, err = io.ReadFull(r, b)
	return b, unexpected(err)
}

func unexpected(err error) error {
	if err == io.EOF {
		return io.ErrUnexpectedEOF
	}
	return err
}

func appendFileList(b []byte, logs, count int) []byte {
	b = binary.AppendUvarint(b, uint64(logs))
	return binary.AppendUvarint(b, uint64(count))
}

func appendFileEntry(b []byte, sf bitcask.SnapshotFile) []byte {
	b = binary.AppendUvarint(b, uint64(len(sf.Path)))
	b = append(b, sf.Path...)
	return binary.AppendUvarint(b, uint64(sf.Size))
}

func readFileList(r *bufio.Reader) (int, []bitcask.SnapshotFile, error) {
	logs, err := binary.ReadUvarint(r)
	if err != nil || logs == 0 || logs > 1<<16 {
		return 0, nil, errSnapshot
	}
	count, err := binary.ReadUvarint(r)
	if err != nil {
		return 0, nil, errSnapshot
	}
	var files []bitcask.SnapshotFile
	for range count {
		path, err := readField(r)
		if err != nil {
			return 0, nil, errSnapshot
		}
		size, err := binary.ReadUvarint(r)
		if err != nil || !filepath.IsLocal(filepath.FromSlash(string(path))) {
			return 0, nil, errSnapshot
		}
		files = append(files, bitcask.SnapshotFile{Path: string(path), Size: int64(size)})
	}
	return int(logs), files, nil
}

func writeSync(path string, b []byte) error {
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o600)
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
