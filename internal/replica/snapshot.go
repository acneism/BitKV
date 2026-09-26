package replica

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
)

const (
	linksMagic      = "bitkv-links 1\n"
	streamMagic     = "bitkv-snap 2\n"
	linksDir        = "links"
	restoreDir      = "restore"
	restoringMarker = "RESTORING"
	retainSnapshots = 2
)

var errSnapshot = errors.New("replica: malformed snapshot")

type linkStore struct {
	*raft.FileSnapshotStore
	dir    string
	mu     sync.Mutex
	active map[string]bool
}

func newLinkStore(raftDir string, logger hclog.Logger) (*linkStore, error) {
	fs, err := raft.NewFileSnapshotStoreWithLogger(raftDir, retainSnapshots, logger)
	if err != nil {
		return nil, err
	}
	s := &linkStore{FileSnapshotStore: fs, dir: filepath.Join(raftDir, linksDir), active: make(map[string]bool)}
	s.reap()
	return s, nil
}

func (s *linkStore) Create(version raft.SnapshotVersion, index, term uint64, configuration raft.Configuration, configurationIndex uint64, trans raft.Transport) (raft.SnapshotSink, error) {
	sink, err := s.FileSnapshotStore.Create(version, index, term, configuration, configurationIndex, trans)
	if err != nil {
		return nil, err
	}
	s.mu.Lock()
	s.active[sink.ID()] = true
	s.mu.Unlock()
	return &linkSink{SnapshotSink: sink, store: s}, nil
}

func (s *linkStore) Open(id string) (*raft.SnapshotMeta, io.ReadCloser, error) {
	meta, rc, err := s.FileSnapshotStore.Open(id)
	if err != nil {
		return nil, nil, err
	}
	br := bufio.NewReader(rc)
	if head, _ := br.Peek(len(linksMagic)); string(head) != linksMagic {
		return meta, readCloser{br, rc}, nil
	}
	br.Discard(len(linksMagic))
	logs, files, err := readFileList(br, nil)
	rc.Close()
	if err != nil {
		return nil, nil, err
	}
	stream, size, err := openStream(filepath.Join(s.dir, id), logs, files)
	if err != nil {
		return nil, nil, err
	}
	meta.Size = size
	return meta, stream, nil
}

func (s *linkStore) reap() {
	metas, err := s.List()
	if err != nil {
		return
	}
	keep := make(map[string]bool, len(metas))
	for _, m := range metas {
		keep[m.ID] = true
	}
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, e := range entries {
		if !keep[e.Name()] && !s.active[e.Name()] {
			os.RemoveAll(filepath.Join(s.dir, e.Name()))
		}
	}
}

func (s *linkStore) finish(id string) {
	s.mu.Lock()
	delete(s.active, id)
	s.mu.Unlock()
}

type linkSink struct {
	raft.SnapshotSink
	store *linkStore
}

func (k *linkSink) dir() string {
	return filepath.Join(k.store.dir, k.ID())
}

func (k *linkSink) Close() error {
	err := k.SnapshotSink.Close()
	k.store.finish(k.ID())
	k.store.reap()
	return err
}

func (k *linkSink) Cancel() error {
	err := k.SnapshotSink.Cancel()
	os.RemoveAll(k.dir())
	k.store.finish(k.ID())
	return err
}

type readCloser struct {
	io.Reader
	io.Closer
}

type multiCloser struct {
	io.Reader
	files []*os.File
}

func (m multiCloser) Close() error {
	var err error
	for _, f := range m.files {
		err = errors.Join(err, f.Close())
	}
	return err
}

func openStream(dir string, logs int, files []bitcask.SnapshotFile) (io.ReadCloser, int64, error) {
	head := appendFileList([]byte(streamMagic), logs, len(files))
	readers := []io.Reader{bytes.NewReader(head)}
	size := int64(len(head))
	var opened []*os.File
	for _, sf := range files {
		f, err := os.Open(filepath.Join(dir, filepath.FromSlash(sf.Path)))
		if err != nil {
			multiCloser{files: opened}.Close()
			return nil, 0, err
		}
		opened = append(opened, f)
		h := appendFileEntry(nil, sf)
		readers = append(readers, bytes.NewReader(h), io.NewSectionReader(f, 0, sf.Size))
		size += int64(len(h)) + sf.Size
	}
	return multiCloser{io.MultiReader(readers...), opened}, size, nil
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

func readFileList(r *bufio.Reader, body func(sf bitcask.SnapshotFile) error) (int, []bitcask.SnapshotFile, error) {
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
		sf := bitcask.SnapshotFile{Path: string(path), Size: int64(size)}
		if body != nil {
			if err := body(sf); err != nil {
				return 0, nil, err
			}
		}
		files = append(files, sf)
	}
	return int(logs), files, nil
}

func (n *Node) Snapshot() (raft.FSMSnapshot, error) {
	return snapshot{n.db}, nil
}

type snapshot struct {
	db *bitcask.DB
}

func (s snapshot) Persist(sink raft.SnapshotSink) error {
	if err := s.persist(sink); err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (s snapshot) persist(sink raft.SnapshotSink) error {
	k, ok := sink.(*linkSink)
	if !ok {
		return fmt.Errorf("replica: snapshot sink %T does not support links", sink)
	}
	logs, files, err := s.db.LinkFiles(k.dir())
	if err != nil {
		return err
	}
	b := appendFileList([]byte(linksMagic), logs, len(files))
	for _, sf := range files {
		b = appendFileEntry(b, sf)
	}
	_, err = sink.Write(b)
	return err
}

func (snapshot) Release() {}

func (n *Node) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	marker := filepath.Join(n.dir, restoringMarker)
	if err := touchSync(marker); err != nil {
		return err
	}
	br := bufio.NewReaderSize(rc, 1<<20)
	var err error
	if head, _ := br.Peek(len(streamMagic)); string(head) == streamMagic {
		br.Discard(len(streamMagic))
		err = n.restoreFiles(br)
	} else {
		err = n.restoreOps(br)
	}
	if err == nil {
		err = n.db.Sync()
	}
	if err == nil {
		err = os.Remove(marker)
	}
	return err
}

func (n *Node) restoreFiles(r *bufio.Reader) error {
	tmp := filepath.Join(n.dir, restoreDir)
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	defer os.RemoveAll(tmp)
	logs, _, err := readFileList(r, func(sf bitcask.SnapshotFile) error {
		dst := filepath.Join(tmp, filepath.FromSlash(sf.Path))
		if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
			return err
		}
		f, err := os.Create(dst)
		if err != nil {
			return err
		}
		_, err = io.CopyN(f, r, sf.Size)
		if cerr := f.Close(); err == nil {
			err = cerr
		}
		return err
	})
	if err != nil {
		return err
	}
	opts := bitcask.DefaultOptions()
	opts.Logs = logs
	opts.Sync = bitcask.SyncNo
	opts.MergeInterval = 0
	opts.ExpireInterval = 0
	opts.Now = n.db.Options().Now
	src, err := bitcask.Open(tmp, opts)
	if err != nil {
		return err
	}
	defer src.Close()
	if err := n.db.Flush(); err != nil {
		return err
	}
	batch := make([]bitcask.Op, 0, restoreBatch)
	err = src.Dump(func(op bitcask.Op) error {
		if batch = append(batch, op); len(batch) < restoreBatch {
			return nil
		}
		err := n.db.Apply(batch, 0)
		batch = batch[:0]
		return err
	})
	if err != nil {
		return err
	}
	return n.db.Apply(batch, 0)
}

func (n *Node) restoreOps(r *bufio.Reader) error {
	if err := n.db.Flush(); err != nil {
		return err
	}
	batch := make([]bitcask.Op, 0, restoreBatch)
	for {
		op, err := readOp(r)
		if err == io.EOF {
			return n.db.Apply(batch, 0)
		}
		if err != nil {
			return err
		}
		if batch = append(batch, op); len(batch) == restoreBatch {
			if err := n.db.Apply(batch, 0); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
}

func touchSync(path string) error {
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	err = f.Sync()
	if cerr := f.Close(); err == nil {
		err = cerr
	}
	if err == nil {
		err = syncDir(filepath.Dir(path))
	}
	return err
}
