package main

import (
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/acneism/BitKV/internal/bitcask"
	"github.com/acneism/BitKV/internal/replica"
	"github.com/acneism/BitKV/internal/server"
)

type config struct {
	addr        string
	dir         string
	fsync       string
	maxBulk     int
	requirePass string
	raftID      string
	raftPeers   string
	raftDir     string
	opts        bitcask.Options
}

func main() {
	cfg := config{opts: bitcask.DefaultOptions()}
	flag.StringVar(&cfg.addr, "addr", "127.0.0.1:6379", "TCP listen address")
	flag.StringVar(&cfg.dir, "dir", "data", "data directory")
	flag.StringVar(&cfg.fsync, "appendfsync", "everysec", "fsync policy: always, everysec or no")
	flag.Int64Var(&cfg.opts.MaxFileSize, "max-file-size", cfg.opts.MaxFileSize, "data file rotation threshold in bytes")
	flag.Float64Var(&cfg.opts.MergeRatio, "merge-ratio", cfg.opts.MergeRatio, "dead bytes ratio that triggers automatic merge")
	flag.Int64Var(&cfg.opts.MergeMinBytes, "merge-min-bytes", cfg.opts.MergeMinBytes, "minimum total size for automatic merge")
	flag.DurationVar(&cfg.opts.MergeInterval, "merge-interval", cfg.opts.MergeInterval, "automatic merge check interval, 0 disables it")
	flag.IntVar(&cfg.opts.Logs, "logs", 0, "number of parallel data logs for a new database (0 means 4; an existing database keeps its own)")
	flag.IntVar(&cfg.maxBulk, "proto-max-bulk-len", 512<<20, "maximum bulk string length in bytes")
	flag.StringVar(&cfg.requirePass, "requirepass", "", "password clients must AUTH with (default from BITKV_REQUIREPASS)")
	flag.StringVar(&cfg.raftID, "raft-id", "", "raft node id; enables replication")
	flag.StringVar(&cfg.raftPeers, "raft-peers", "", "all raft nodes including this one: id=host:port,id=host:port")
	flag.StringVar(&cfg.raftDir, "raft-dir", "", "raft log and snapshot directory (default <dir>/raft)")
	flag.Parse()
	if cfg.requirePass == "" {
		cfg.requirePass = os.Getenv("BITKV_REQUIREPASS")
	}

	logger := slog.New(slog.NewTextHandler(os.Stderr, nil))
	if err := run(logger, cfg); err != nil {
		logger.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger, cfg config) error {
	policy, err := bitcask.ParseSyncPolicy(cfg.fsync)
	if err != nil {
		return err
	}
	cfg.opts.Sync = policy

	started := time.Now()
	db, err := bitcask.Open(cfg.dir, cfg.opts)
	if err != nil {
		return err
	}
	st := db.Stats()
	logger.Info("database loaded", "dir", cfg.dir, "keys", st.Keys, "logs", st.Logs, "files", st.DataFiles, "took", time.Since(started).Round(time.Millisecond))

	var rep *replica.Node
	if cfg.raftID != "" {
		if rep, err = openReplica(cfg, db); err != nil {
			db.Close()
			return err
		}
		logger.Info("raft started", "id", cfg.raftID, "peers", cfg.raftPeers)
	}

	ln, err := net.Listen("tcp", cfg.addr)
	if err != nil {
		closeStore(rep, db)
		return err
	}
	if cfg.requirePass == "" && !isLoopback(ln.Addr()) {
		logger.Warn("listening on a non-loopback address without a password; set -requirepass", "addr", ln.Addr().String())
	}
	srv := server.New(db, server.Config{MaxBulkLen: cfg.maxBulk, RequirePass: cfg.requirePass, Logger: logger, Replica: rep})
	serveErr := make(chan error, 1)
	go func() { serveErr <- srv.Serve(ln) }()
	logger.Info("ready to accept connections", "addr", ln.Addr().String(), "appendfsync", policy.String(),
		"auth", cfg.requirePass != "", "version", server.Version)

	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	select {
	case sig := <-signals:
		logger.Info("shutting down", "signal", sig.String())
	case err = <-serveErr:
		if errors.Is(err, server.ErrServerClosed) {
			err = nil
		}
	}
	srv.Close()
	if cerr := closeStore(rep, db); cerr != nil && err == nil {
		err = cerr
	}
	if err == nil {
		logger.Info("bye")
	}
	return err
}

func isLoopback(addr net.Addr) bool {
	tcp, ok := addr.(*net.TCPAddr)
	return ok && tcp.IP.IsLoopback()
}

func openReplica(cfg config, db *bitcask.DB) (*replica.Node, error) {
	peers, err := replica.ParsePeers(cfg.raftPeers)
	if err != nil {
		return nil, err
	}
	dir := cfg.raftDir
	if dir == "" {
		dir = filepath.Join(cfg.dir, "raft")
	}
	return replica.Open(db, replica.Config{ID: cfg.raftID, Peers: peers, Dir: dir, LogOutput: os.Stderr})
}

func closeStore(rep *replica.Node, db *bitcask.DB) error {
	var err error
	if rep != nil {
		err = rep.Close()
	}
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	return err
}
