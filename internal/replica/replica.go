package replica

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

var (
	ErrNotLeader      = errors.New("replica: not the leader")
	ErrLeadershipLost = errors.New("replica: write interrupted by a leadership change, it may or may not be applied")
	ErrNotEmpty       = errors.New("replica: database has data but no raft state, start the node from an empty directory")
	ErrOldRaftLog     = errors.New("replica: the raft directory was written by hashicorp/raft (CasketDB v0.9 or older), start the node from an empty directory")
)

type Config struct {
	ID            string
	Peers         map[string]string
	Dir           string
	LogOutput     io.Writer
	UnsafeNoFsync bool
	TLS           *tls.Config
}

type Status struct {
	State      string
	Term       uint64
	Applied    uint64
	LeaderID   string
	LeaderAddr string
}

type Node struct {
	db      *bitcask.DB
	id      raft.NodeID
	peers   map[string]string
	rn      *node.Node
	fsm     *bitcaskFSM
	flushMu sync.RWMutex
	mu      sync.Mutex
	ready   uint64

	wg sync.WaitGroup
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
	return open(db, cfg, nil)
}

func open(db *bitcask.DB, cfg Config, tune func(*node.Config)) (*Node, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("replica: node %q is not in the peer list", cfg.ID)
	}
	if fileExists(filepath.Join(cfg.Dir, "raft.db")) || fileExists(filepath.Join(cfg.Dir, "wal", "wal-meta.db")) {
		return nil, ErrOldRaftLog
	}
	if !fileExists(filepath.Join(cfg.Dir, "wal", "meta")) && db.Len() > 0 {
		return nil, ErrNotEmpty
	}
	fsm, err := newBitcaskFSM(db, cfg.Dir)
	if err != nil {
		return nil, err
	}
	out := cfg.LogOutput
	if out == nil {
		out = io.Discard
	}
	peers := make(map[raft.NodeID]string, len(cfg.Peers))
	for id, addr := range cfg.Peers {
		peers[raft.NodeID(id)] = addr
	}
	nc := node.Config{
		ID:              raft.NodeID(cfg.ID),
		Dir:             cfg.Dir,
		Peers:           peers,
		TLS:             cfg.TLS,
		StateMachine:    fsm,
		TickInterval:    10 * time.Millisecond,
		ElectionTicks:   100,
		HeartbeatTicks:  10,
		PreVote:         true,
		CheckQuorum:     true,
		NoSync:          cfg.UnsafeNoFsync,
		CompactEntries:  1 << 16,
		TrailingEntries: 1 << 16,
		Logger:          slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelWarn})).With("component", "raft"),
	}
	if tune != nil {
		tune(&nc)
	}
	rn, err := node.Open(nc)
	if err != nil {
		return nil, err
	}
	n := &Node{db: db, id: nc.ID, peers: cfg.Peers, rn: rn, fsm: fsm}
	n.wg.Add(1)
	go n.watch()
	return n, nil
}

func (n *Node) watch() {
	defer n.wg.Done()
	for e := range n.rn.Events() {
		n.mu.Lock()
		n.ready = 0
		n.mu.Unlock()
		n.db.DropProposed()
		if e.Ready && e.Leader == n.id {
			n.mu.Lock()
			n.ready = e.Term
			n.mu.Unlock()
		}
	}
}

func (n *Node) term() uint64 {
	n.mu.Lock()
	defer n.mu.Unlock()
	return n.ready
}

func (n *Node) propose(term uint64, data []byte) (node.Proposal, error) {
	p, err := n.rn.ProposeIn(term, data)
	switch {
	case errors.Is(err, node.ErrNotLeader):
		return p, ErrNotLeader
	case err != nil:
		return p, ErrLeadershipLost
	}
	return p, nil
}

func (n *Node) Update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	n.flushMu.RLock()
	defer n.flushMu.RUnlock()
	term := n.term()
	if term == 0 {
		return ErrNotLeader
	}
	var p node.Proposal
	err := n.db.Propose(scope, term, fn, func(ops []bitcask.Op) (uint64, error) {
		var err error
		p, err = n.propose(term, encodeEntry(kindOps, ops))
		return p.Index, err
	})
	if err != nil || p.Index == 0 {
		return err
	}
	return n.wait(p)
}

func (n *Node) Flush() error {
	n.flushMu.Lock()
	defer n.flushMu.Unlock()
	term := n.term()
	if term == 0 {
		return ErrNotLeader
	}
	p, err := n.propose(term, encodeEntry(kindFlush, nil))
	if err != nil {
		return err
	}
	return n.wait(p)
}

func (n *Node) wait(p node.Proposal) error {
	if err := n.rn.Wait(context.Background(), p); err != nil {
		return ErrLeadershipLost
	}
	return nil
}

func (n *Node) Status() Status {
	st := n.rn.Status()
	return Status{
		State:      st.State.String(),
		Term:       st.Term,
		Applied:    st.Applied,
		LeaderID:   string(st.Lead),
		LeaderAddr: n.peers[string(st.Lead)],
	}
}

func (n *Node) Close() error {
	err := n.rn.Close()
	n.wg.Wait()
	return err
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}
