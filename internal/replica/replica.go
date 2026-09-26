package replica

import (
	"bytes"
	"crypto/rand"
	"crypto/tls"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/hashicorp/go-hclog"
	"github.com/hashicorp/raft"
	wal "github.com/hashicorp/raft-wal"
	"github.com/hashicorp/raft-wal/segment"
	"github.com/hashicorp/raft-wal/types"
)

var (
	ErrNotLeader      = errors.New("replica: not the leader")
	ErrLeadershipLost = errors.New("replica: write interrupted by a leadership change, it may or may not be applied")
	ErrNotEmpty       = errors.New("replica: database has data but no raft state, start the node from an empty directory")
	ErrOldRaftLog     = errors.New("replica: raft.db from v0.5 found, the raft log is now raft-wal; start the node from an empty directory")
	ErrOtherEngine    = errors.New("replica: the raft directory was written by the other raft engine; start the node from an empty directory")
	errStale          = errors.New("replica: entry was proposed in another term")
	errEntry          = errors.New("replica: malformed log entry")
)

const (
	kindOps    byte = 1
	kindFlush  byte = 2
	flagDelete byte = 1

	maxField          = 1<<32 - 1
	restoreBatch      = 1024
	snapshotThreshold = 1 << 20
)

const (
	EngineHashicorp = "hashicorp"
	EngineOwn       = "own"
)

type Config struct {
	ID            string
	Peers         map[string]string
	Dir           string
	LogOutput     io.Writer
	UnsafeNoFsync bool
	Engine        string
	TLS           *tls.Config
}

type Replica interface {
	Update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error
	Flush() error
	Status() Status
	Close() error
}

type Status struct {
	State      string
	Term       uint64
	Applied    uint64
	LeaderID   string
	LeaderAddr string
}

type Node struct {
	db    *bitcask.DB
	dir   string
	raft  *raft.Raft
	store *wal.WAL
	base  uint64
	seq   atomic.Uint64

	flushMu sync.RWMutex

	mu    sync.Mutex
	ready uint64

	stop chan struct{}
	wg   sync.WaitGroup
}

func ParsePeers(s string) (map[string]string, error) {
	peers := make(map[string]string)
	for _, part := range strings.Split(s, ",") {
		id, addr, ok := strings.Cut(strings.TrimSpace(part), "=")
		if !ok || id == "" || addr == "" {
			return nil, fmt.Errorf("replica: bad peer %q, want id=host:port", part)
		}
		if _, dup := peers[id]; dup {
			return nil, fmt.Errorf("replica: duplicate peer id %q", id)
		}
		peers[id] = addr
	}
	return peers, nil
}

func Open(db *bitcask.DB, cfg Config) (Replica, error) {
	switch cfg.Engine {
	case "", EngineHashicorp:
		if cfg.TLS != nil {
			return nil, errors.New("replica: the hashicorp engine does not support TLS")
		}
		if fileExists(filepath.Join(cfg.Dir, "wal", "meta")) {
			return nil, ErrOtherEngine
		}
		rc := raft.DefaultConfig()
		rc.SnapshotThreshold = snapshotThreshold
		return open(db, cfg, rc, wal.DefaultSegmentSize)
	case EngineOwn:
		return openOwn(db, cfg, nil)
	}
	return nil, fmt.Errorf("replica: unknown engine %q, want %s or %s", cfg.Engine, EngineHashicorp, EngineOwn)
}

func open(db *bitcask.DB, cfg Config, rc *raft.Config, segmentSize int) (*Node, error) {
	addr, ok := cfg.Peers[cfg.ID]
	if !ok {
		return nil, fmt.Errorf("replica: node %q is not in the peer list", cfg.ID)
	}
	out := cfg.LogOutput
	if out == nil {
		out = io.Discard
	}
	logger := hclog.New(&hclog.LoggerOptions{Name: "raft", Level: hclog.Warn, Output: out})
	rc.LocalID = raft.ServerID(cfg.ID)
	rc.BatchApplyCh = true
	rc.Logger = logger
	rc.NoSnapshotRestoreOnStart = db.Len() > 0 && !fileExists(filepath.Join(cfg.Dir, restoringMarker))
	if _, err := os.Stat(filepath.Join(cfg.Dir, "raft.db")); err == nil {
		return nil, ErrOldRaftLog
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	snaps, err := newLinkStore(cfg.Dir, logger)
	if err != nil {
		return nil, err
	}
	walDir := filepath.Join(cfg.Dir, "wal")
	if err := os.MkdirAll(walDir, 0o755); err != nil {
		return nil, err
	}
	if err := initWalMeta(walDir); err != nil {
		return nil, err
	}
	vfs := newWalFS()
	if cfg.UnsafeNoFsync {
		vfs = noSyncFS{vfs}
	}
	store, err := wal.Open(walDir, wal.WithLogger(logger), wal.WithSegmentFiler(segment.NewFiler(walDir, vfs)), wal.WithSegmentSize(segmentSize))
	if err != nil {
		return nil, err
	}
	existing, err := raft.HasExistingState(store, store, snaps)
	if err == nil && !existing && db.Len() > 0 {
		err = ErrNotEmpty
	}
	if err != nil {
		store.Close()
		return nil, err
	}
	trans, err := raft.NewTCPTransportWithLogger(addr, nil, 3, 10*time.Second, logger)
	if err != nil {
		store.Close()
		return nil, err
	}
	var seed [8]byte
	if _, err := rand.Read(seed[:]); err != nil {
		trans.Close()
		store.Close()
		return nil, err
	}
	n := &Node{
		db:    db,
		dir:   cfg.Dir,
		store: store,
		base:  binary.LittleEndian.Uint64(seed[:]),
		stop:  make(chan struct{}),
	}
	if n.raft, err = raft.NewRaft(rc, n, store, store, snaps, trans); err != nil {
		trans.Close()
		store.Close()
		return nil, err
	}
	if !existing {
		servers := make([]raft.Server, 0, len(cfg.Peers))
		for id, a := range cfg.Peers {
			servers = append(servers, raft.Server{ID: raft.ServerID(id), Address: raft.ServerAddress(a)})
		}
		slices.SortFunc(servers, func(a, b raft.Server) int { return strings.Compare(string(a.ID), string(b.ID)) })
		if err := n.raft.BootstrapCluster(raft.Configuration{Servers: servers}).Error(); err != nil && !errors.Is(err, raft.ErrCantBootstrap) {
			n.raft.Shutdown()
			store.Close()
			return nil, err
		}
	}
	n.wg.Add(1)
	go n.watch()
	return n, nil
}

func (n *Node) Close() error {
	close(n.stop)
	err := n.raft.Shutdown().Error()
	n.wg.Wait()
	if cerr := n.store.Close(); err == nil {
		err = cerr
	}
	return err
}

func (n *Node) Status() Status {
	addr, id := n.raft.LeaderWithID()
	return Status{
		State:      n.raft.State().String(),
		Term:       n.raft.CurrentTerm(),
		Applied:    n.raft.AppliedIndex(),
		LeaderID:   string(id),
		LeaderAddr: string(addr),
	}
}

func (n *Node) watch() {
	defer n.wg.Done()
	for {
		var leader bool
		select {
		case <-n.stop:
			return
		case leader = <-n.raft.LeaderCh():
		}
		n.mu.Lock()
		n.ready = 0
		n.mu.Unlock()
		n.db.DropProposed()
		if !leader {
			continue
		}
		term := n.raft.CurrentTerm()
		if n.raft.Barrier(0).Error() != nil {
			continue
		}
		n.mu.Lock()
		if n.raft.CurrentTerm() == term && n.raft.State() == raft.Leader {
			n.ready = term
		}
		n.mu.Unlock()
	}
}

func (n *Node) lease() (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ready == 0 || n.ready != n.raft.CurrentTerm() || n.raft.State() != raft.Leader {
		return 0, ErrNotLeader
	}
	return n.ready, nil
}

func (n *Node) Update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	n.flushMu.RLock()
	defer n.flushMu.RUnlock()
	term, err := n.lease()
	if err != nil {
		return err
	}
	var f raft.ApplyFuture
	err = n.db.Propose(scope, term, fn, func(ops []bitcask.Op) (uint64, error) {
		id := n.base + n.seq.Add(1)
		f = n.raft.Apply(encodeEntry(kindOps, term, id, ops), 0)
		return id, nil
	})
	if err != nil || f == nil {
		return err
	}
	return n.wait(f)
}

func (n *Node) Flush() error {
	n.flushMu.Lock()
	defer n.flushMu.Unlock()
	term, err := n.lease()
	if err != nil {
		return err
	}
	return n.wait(n.raft.Apply(encodeEntry(kindFlush, term, n.base+n.seq.Add(1), nil), 0))
}

func (n *Node) wait(f raft.ApplyFuture) error {
	done := make(chan error, 1)
	go func() { done <- f.Error() }()
	var err error
	select {
	case err = <-done:
	case <-n.stop:
		return ErrLeadershipLost
	}
	switch {
	case errors.Is(err, raft.ErrNotLeader):
		return ErrNotLeader
	case err != nil:
		return ErrLeadershipLost
	}
	switch err, _ := f.Response().(error); {
	case errors.Is(err, errStale):
		return ErrLeadershipLost
	default:
		return err
	}
}

func (n *Node) Apply(l *raft.Log) any {
	return n.ApplyBatch([]*raft.Log{l})[0]
}

func (n *Node) ApplyBatch(logs []*raft.Log) []any {
	out := make([]any, len(logs))
	var ops []bitcask.Op
	var merged []int
	var upTo uint64
	flush := func() {
		if len(merged) == 0 {
			return
		}
		if err := n.db.Apply(ops, upTo); err != nil {
			for _, i := range merged {
				out[i] = err
			}
		}
		ops, merged, upTo = ops[:0], merged[:0], 0
	}
	for i, l := range logs {
		if l.Type != raft.LogCommand {
			continue
		}
		kind, term, id, body, err := decodeEntry(l.Data)
		if err != nil {
			out[i] = err
			continue
		}
		if term != l.Term {
			out[i] = errStale
			continue
		}
		if kind == kindFlush {
			flush()
			out[i] = n.db.Flush()
			continue
		}
		decoded, err := decodeOps(body)
		if err != nil {
			out[i] = err
			continue
		}
		ops = append(ops, decoded...)
		merged = append(merged, i)
		upTo = max(upTo, id)
	}
	flush()
	return out
}

func encodeEntry(kind byte, term, id uint64, ops []bitcask.Op) []byte {
	size := 1 + 2*binary.MaxVarintLen64
	for _, op := range ops {
		size += 1 + 3*binary.MaxVarintLen64 + len(op.Key) + len(op.Value)
	}
	b := append(make([]byte, 0, size), kind)
	b = binary.AppendUvarint(b, term)
	b = binary.AppendUvarint(b, id)
	for _, op := range ops {
		b = appendOp(b, op)
	}
	return b
}

func decodeEntry(data []byte) (kind byte, term, id uint64, body *bytes.Reader, err error) {
	body = bytes.NewReader(data)
	if kind, err = body.ReadByte(); err != nil {
		return 0, 0, 0, nil, errEntry
	}
	if kind != kindOps && kind != kindFlush {
		return 0, 0, 0, nil, errEntry
	}
	if term, err = binary.ReadUvarint(body); err != nil {
		return 0, 0, 0, nil, errEntry
	}
	if id, err = binary.ReadUvarint(body); err != nil {
		return 0, 0, 0, nil, errEntry
	}
	return kind, term, id, body, nil
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

type noSyncFS struct {
	types.VFS
}

func (fs noSyncFS) Create(dir, name string, size uint64) (types.WritableFile, error) {
	f, err := fs.VFS.Create(dir, name, size)
	if err != nil {
		return nil, err
	}
	return noSyncFile{f}, nil
}

func (fs noSyncFS) OpenWriter(dir, name string) (types.WritableFile, error) {
	f, err := fs.VFS.OpenWriter(dir, name)
	if err != nil {
		return nil, err
	}
	return noSyncFile{f}, nil
}

type noSyncFile struct {
	types.WritableFile
}

func (noSyncFile) Sync() error {
	return nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
