package replica

import (
	"bufio"
	"bytes"
	"crypto/rand"
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
)

var (
	ErrNotLeader      = errors.New("replica: not the leader")
	ErrLeadershipLost = errors.New("replica: write interrupted by a leadership change, it may or may not be applied")
	ErrNotEmpty       = errors.New("replica: database has data but no raft state, start the node from an empty directory")
	ErrOldRaftLog     = errors.New("replica: raft.db from v0.5 found, the raft log is now raft-wal; start the node from an empty directory")
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

type Config struct {
	ID        string
	Peers     map[string]string
	Dir       string
	LogOutput io.Writer
}

type Status struct {
	State      string
	Term       uint64
	Applied    uint64
	LeaderID   string
	LeaderAddr string
}

type proposal struct {
	commit func() error
	err    error
	done   chan struct{}
}

type Node struct {
	db    *bitcask.DB
	raft  *raft.Raft
	store *wal.WAL
	base  uint64
	seq   atomic.Uint64

	flushMu sync.RWMutex

	mu      sync.Mutex
	pending map[uint64]*proposal
	epoch   chan struct{}
	ready   uint64

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

func Open(db *bitcask.DB, cfg Config) (*Node, error) {
	rc := raft.DefaultConfig()
	rc.SnapshotThreshold = snapshotThreshold
	return open(db, cfg, rc)
}

func open(db *bitcask.DB, cfg Config, rc *raft.Config) (*Node, error) {
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
	rc.Logger = logger
	rc.NoSnapshotRestoreOnStart = db.Len() > 0
	if _, err := os.Stat(filepath.Join(cfg.Dir, "raft.db")); err == nil {
		return nil, ErrOldRaftLog
	}
	if err := os.MkdirAll(cfg.Dir, 0o755); err != nil {
		return nil, err
	}
	snaps, err := raft.NewFileSnapshotStoreWithLogger(cfg.Dir, 2, logger)
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
	store, err := wal.Open(walDir, wal.WithLogger(logger), wal.WithSegmentFiler(segment.NewFiler(walDir, newWalFS())))
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
		db:      db,
		store:   store,
		base:    binary.LittleEndian.Uint64(seed[:]),
		pending: make(map[uint64]*proposal),
		epoch:   make(chan struct{}),
		stop:    make(chan struct{}),
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
		close(n.epoch)
		n.epoch = make(chan struct{})
		n.ready = 0
		n.mu.Unlock()
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

func (n *Node) lease() (uint64, <-chan struct{}, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.ready == 0 || n.ready != n.raft.CurrentTerm() || n.raft.State() != raft.Leader {
		return 0, nil, ErrNotLeader
	}
	return n.ready, n.epoch, nil
}

func (n *Node) Update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	n.flushMu.RLock()
	defer n.flushMu.RUnlock()
	term, epoch, err := n.lease()
	if err != nil {
		return err
	}
	return n.db.UpdateVia(scope, fn, func(ops []bitcask.Op, commit func() error) error {
		if len(ops) == 0 {
			return commit()
		}
		return n.propose(term, epoch, kindOps, ops, commit)
	})
}

func (n *Node) Flush() error {
	n.flushMu.Lock()
	defer n.flushMu.Unlock()
	term, epoch, err := n.lease()
	if err != nil {
		return err
	}
	return n.propose(term, epoch, kindFlush, nil, n.db.Flush)
}

func (n *Node) propose(term uint64, epoch <-chan struct{}, kind byte, ops []bitcask.Op, commit func() error) error {
	id := n.base + n.seq.Add(1)
	p := &proposal{commit: commit, done: make(chan struct{})}
	n.mu.Lock()
	n.pending[id] = p
	n.mu.Unlock()
	n.raft.Apply(encodeEntry(kind, term, id, ops), 0)
	select {
	case <-p.done:
		return p.err
	case <-epoch:
	case <-n.stop:
	}
	n.mu.Lock()
	_, waiting := n.pending[id]
	delete(n.pending, id)
	n.mu.Unlock()
	if waiting {
		return ErrLeadershipLost
	}
	<-p.done
	return p.err
}

func (n *Node) Apply(l *raft.Log) any {
	kind, term, id, body, err := decodeEntry(l.Data)
	if err != nil {
		return err
	}
	if term != l.Term {
		return errStale
	}
	n.mu.Lock()
	p := n.pending[id]
	delete(n.pending, id)
	n.mu.Unlock()
	if p != nil {
		p.err = p.commit()
		close(p.done)
		return p.err
	}
	if kind == kindFlush {
		return n.db.Flush()
	}
	ops, err := decodeOps(body)
	if err != nil {
		return err
	}
	return n.db.Apply(ops)
}

func (n *Node) Snapshot() (raft.FSMSnapshot, error) {
	return dump{n.db}, nil
}

func (n *Node) Restore(rc io.ReadCloser) error {
	defer rc.Close()
	if err := n.db.Flush(); err != nil {
		return err
	}
	r := bufio.NewReader(rc)
	batch := make([]bitcask.Op, 0, restoreBatch)
	for {
		op, err := readOp(r)
		if err == io.EOF {
			return n.db.Apply(batch)
		}
		if err != nil {
			return err
		}
		if batch = append(batch, op); len(batch) == restoreBatch {
			if err := n.db.Apply(batch); err != nil {
				return err
			}
			batch = batch[:0]
		}
	}
}

type dump struct {
	db *bitcask.DB
}

func (d dump) Persist(sink raft.SnapshotSink) error {
	w := bufio.NewWriter(sink)
	var buf []byte
	err := d.db.Sync()
	if err == nil {
		err = d.db.Dump(func(op bitcask.Op) error {
			buf = appendOp(buf[:0], op)
			_, err := w.Write(buf)
			return err
		})
	}
	if err == nil {
		err = w.Flush()
	}
	if err != nil {
		sink.Cancel()
		return err
	}
	return sink.Close()
}

func (dump) Release() {}

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
