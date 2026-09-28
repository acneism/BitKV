package main

import (
	"errors"
	"flag"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/replica"
	"github.com/acneism/casketdb/internal/server"
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
	raftNoFsync bool
	raftReads   string
	raftDrift   float64
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
	flag.StringVar(&cfg.requirePass, "requirepass", "", "password clients must AUTH with (default from CASKETDB_REQUIREPASS)")
	flag.StringVar(&cfg.raftID, "raft-id", "", "raft node id; enables replication")
	flag.StringVar(&cfg.raftPeers, "raft-peers", "", "all raft nodes including this one: id=host:port,id=host:port")
	flag.StringVar(&cfg.raftDir, "raft-dir", "", "raft log and snapshot directory (default <dir>/raft)")
	flag.BoolVar(&cfg.raftNoFsync, "raft-unsafe-no-fsync", false, "skip fsync of the raft log: faster, but a power loss on one node followed by a leader failure can lose acknowledged writes")
	flag.StringVar(&cfg.raftReads, "raft-reads", "local", "read consistency in a cluster: local (may be stale), linearizable (confirmed by the leader) or lease (the leader answers from its lease)")
	flag.Float64Var(&cfg.raftDrift, "raft-max-clock-drift", 0.1, "largest relative difference between node clock rates that -raft-reads lease tolerates")
	flag.Parse()
	if cfg.requirePass == "" {
		cfg.requirePass = os.Getenv("CASKETDB_REQUIREPASS")
	}
	if cfg.raftDir == "" {
		cfg.raftDir = filepath.Join(cfg.dir, "raft")
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
	warnIfShared(logger, cfg.dir)

	var rep *replica.Node
	if cfg.raftID != "" {
		if rep, err = openReplica(cfg, db); err != nil {
			db.Close()
			return err
		}
		logger.Info("raft started", "id", cfg.raftID, "peers", cfg.raftPeers)
		warnIfShared(logger, cfg.raftDir)
		if cfg.raftNoFsync {
			logger.Warn("raft log fsync is disabled (-raft-unsafe-no-fsync): a power loss can lose acknowledged writes")
		}
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

func warnIfShared(logger *slog.Logger, dir string) {
	if runtime.GOOS == "windows" {
		return
	}
	if fi, err := os.Stat(dir); err == nil && fi.Mode().Perm()&0o077 != 0 {
		logger.Warn("directory is accessible to other users; restrict it with chmod 700", "dir", dir, "mode", fi.Mode().Perm().String())
	}
}

func openReplica(cfg config, db *bitcask.DB) (*replica.Node, error) {
	peers, err := replica.ParsePeers(cfg.raftPeers)
	if err != nil {
		return nil, err
	}
	reads, err := replica.ParseReadMode(cfg.raftReads)
	if err != nil {
		return nil, err
	}
	return replica.Open(db, replica.Config{
		ID:            cfg.raftID,
		Peers:         peers,
		Dir:           cfg.raftDir,
		LogOutput:     os.Stderr,
		UnsafeNoFsync: cfg.raftNoFsync,
		Reads:         reads,
		MaxClockDrift: cfg.raftDrift,
	})
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
