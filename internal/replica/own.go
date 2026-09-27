package replica

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"time"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/acneism/raft"
	"github.com/acneism/raft/node"
)

type ownNode struct {
	db      *bitcask.DB
	id      raft.NodeID
	peers   map[string]string
	n       *node.Node
	fsm     *bitcaskFSM
	flushMu sync.RWMutex
	mu      sync.Mutex
	ready   uint64

	wg sync.WaitGroup
}

func openOwn(db *bitcask.DB, cfg Config, tune func(*node.Config)) (*ownNode, error) {
	if _, ok := cfg.Peers[cfg.ID]; !ok {
		return nil, fmt.Errorf("replica: node %q is not in the peer list", cfg.ID)
	}
	if fileExists(filepath.Join(cfg.Dir, "raft.db")) {
		return nil, ErrOldRaftLog
	}
	if fileExists(filepath.Join(cfg.Dir, "wal", "wal-meta.db")) {
		return nil, ErrOtherEngine
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
		Logger:          slog.New(slog.NewTextHandler(out, &slog.HandlerOptions{Level: slog.LevelWarn})).With("raft", "own"),
	}
	if tune != nil {
		tune(&nc)
	}
	n, err := node.Open(nc)
	if err != nil {
		return nil, err
	}
	o := &ownNode{db: db, id: nc.ID, peers: cfg.Peers, n: n, fsm: fsm}
	o.wg.Add(1)
	go o.watch()
	return o, nil
}

func (o *ownNode) watch() {
	defer o.wg.Done()
	for e := range o.n.Events() {
		o.mu.Lock()
		o.ready = 0
		o.mu.Unlock()
		o.db.DropProposed()
		if e.Ready && e.Leader == o.id {
			o.mu.Lock()
			o.ready = e.Term
			o.mu.Unlock()
		}
	}
}

func (o *ownNode) term() uint64 {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.ready
}

func (o *ownNode) propose(term uint64, data []byte) (node.Proposal, error) {
	p, err := o.n.ProposeIn(term, data)
	switch {
	case errors.Is(err, node.ErrNotLeader):
		return p, ErrNotLeader
	case err != nil:
		return p, ErrLeadershipLost
	}
	return p, nil
}

func (o *ownNode) Update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	o.flushMu.RLock()
	defer o.flushMu.RUnlock()
	term := o.term()
	if term == 0 {
		return ErrNotLeader
	}
	var p node.Proposal
	err := o.db.Propose(scope, term, fn, func(ops []bitcask.Op) (uint64, error) {
		var err error
		p, err = o.propose(term, encodeOwnEntry(kindOps, ops))
		return p.Index, err
	})
	if err != nil || p.Index == 0 {
		return err
	}
	return o.wait(p)
}

func (o *ownNode) Flush() error {
	o.flushMu.Lock()
	defer o.flushMu.Unlock()
	term := o.term()
	if term == 0 {
		return ErrNotLeader
	}
	p, err := o.propose(term, encodeOwnEntry(kindFlush, nil))
	if err != nil {
		return err
	}
	return o.wait(p)
}

func (o *ownNode) wait(p node.Proposal) error {
	if err := o.n.Wait(context.Background(), p); err != nil {
		return ErrLeadershipLost
	}
	return nil
}

func (o *ownNode) Status() Status {
	st := o.n.Status()
	return Status{
		State:      st.State.String(),
		Term:       st.Term,
		Applied:    st.Applied,
		LeaderID:   string(st.Lead),
		LeaderAddr: o.peers[string(st.Lead)],
	}
}

func (o *ownNode) Close() error {
	err := o.n.Close()
	o.wg.Wait()
	return err
}
