package server

import (
	"crypto/subtle"
	"errors"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/acneism/casketdb/internal/bitcask"
	"github.com/acneism/casketdb/internal/replica"
	"github.com/acneism/casketdb/internal/resp"
)

const (
	Version      = "0.10.0"
	redisVersion = "7.2.0"
)

var ErrServerClosed = errors.New("server: closed")

type Config struct {
	MaxBulkLen  int
	RequirePass string
	Logger      *slog.Logger
	Replica     *replica.Node
}

type Server struct {
	db      *bitcask.DB
	cfg     Config
	log     *slog.Logger
	started time.Time

	mu        sync.Mutex
	listeners map[net.Listener]struct{}
	clients   map[*client]struct{}
	closed    bool
	wg        sync.WaitGroup

	nextID      atomic.Int64
	connections atomic.Int64
	processed   atomic.Int64
}

type client struct {
	id      int64
	conn    net.Conn
	r       *resp.Reader
	w       *resp.Writer
	name    string
	authed  bool
	quit    bool
	multi   bool
	dirty   bool
	queue   []queued
	watched map[string]bitcask.Version
}

func New(db *bitcask.DB, cfg Config) *Server {
	if cfg.MaxBulkLen <= 0 {
		cfg.MaxBulkLen = 512 << 20
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Server{
		db:        db,
		cfg:       cfg,
		log:       logger,
		started:   time.Now(),
		listeners: make(map[net.Listener]struct{}),
		clients:   make(map[*client]struct{}),
	}
}

func (s *Server) Serve(ln net.Listener) error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		ln.Close()
		return ErrServerClosed
	}
	s.listeners[ln] = struct{}{}
	s.mu.Unlock()
	var backoff time.Duration
	for {
		conn, err := ln.Accept()
		if err != nil {
			if s.isClosed() {
				return ErrServerClosed
			}
			if errors.Is(err, net.ErrClosed) {
				return err
			}
			backoff = min(max(2*backoff, 5*time.Millisecond), time.Second)
			s.log.Warn("accept failed", "err", err, "retry_in", backoff)
			time.Sleep(backoff)
			continue
		}
		backoff = 0
		c := &client{
			id:     s.nextID.Add(1),
			conn:   conn,
			r:      resp.NewReader(conn, s.cfg.MaxBulkLen),
			w:      resp.NewWriter(conn),
			authed: s.cfg.RequirePass == "",
		}
		s.mu.Lock()
		if s.closed {
			s.mu.Unlock()
			conn.Close()
			return ErrServerClosed
		}
		s.clients[c] = struct{}{}
		s.wg.Add(1)
		s.mu.Unlock()
		s.connections.Add(1)
		go s.serveClient(c)
	}
}

func (s *Server) Close() error {
	s.mu.Lock()
	s.closed = true
	for ln := range s.listeners {
		ln.Close()
	}
	for c := range s.clients {
		c.conn.Close()
	}
	s.mu.Unlock()
	s.wg.Wait()
	return nil
}

func (s *Server) isClosed() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.closed
}

func (s *Server) clientCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.clients)
}

func (s *Server) checkAuth(user string, pass []byte) bool {
	if user != "default" {
		return false
	}
	return s.cfg.RequirePass == "" || subtle.ConstantTimeCompare(pass, []byte(s.cfg.RequirePass)) == 1
}

func (s *Server) serveClient(c *client) {
	defer s.wg.Done()
	defer func() {
		s.mu.Lock()
		delete(s.clients, c)
		s.mu.Unlock()
		c.conn.Close()
	}()
	var batch []queued
	for !c.quit {
		args, err := c.r.ReadCommand()
		if err != nil {
			s.runBatch(c, batch)
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				c.w.Error("ERR " + pe.Error())
			}
			c.w.Flush()
			return
		}
		cmd, ok := s.batchable(c, args)
		if ok {
			batch = append(batch, queued{cmd: cmd, args: args})
			if c.r.Ready() {
				continue
			}
		}
		s.runBatch(c, batch)
		batch = batch[:0]
		if !ok && len(args) > 0 {
			s.execute(c, args)
		}
		if c.quit || c.r.Buffered() == 0 {
			if err := c.w.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Server) batchable(c *client, args [][]byte) (command, bool) {
	if len(args) == 0 || c.multi || !c.authed {
		return command{}, false
	}
	cmd, ok := lookup(args[0])
	if !ok || cmd.kind != kindWrite || cmd.global || !cmd.validArity(len(args)) {
		return command{}, false
	}
	return cmd, true
}

func (s *Server) runBatch(c *client, batch []queued) {
	switch len(batch) {
	case 0:
		return
	case 1:
		s.execute(c, batch[0].args)
		return
	}
	defer s.recoverCommand(c, batch[0].args[0])
	var keys []string
	for _, q := range batch {
		keys = q.cmd.keys.extract(q.args, keys)
	}
	var replies arrayReply
	err := s.update(bitcask.Keys(keys...), func(tx *bitcask.Tx) (err error) {
		replies, err = runQueue(tx, batch)
		return err
	})
	s.processed.Add(int64(len(batch)))
	for i := range batch {
		if err != nil {
			storageError(err).writeTo(c.w)
		} else {
			replies[i].writeTo(c.w)
		}
	}
}

func (s *Server) recoverCommand(c *client, name []byte) {
	if r := recover(); r != nil {
		s.log.Error("command panicked", "cmd", truncate(name, 64), "panic", r)
		c.w.Error("ERR internal error")
		c.quit = true
	}
}

func (s *Server) execute(c *client, args [][]byte) {
	defer s.recoverCommand(c, args[0])
	cmd, ok := lookup(args[0])
	switch {
	case !ok:
		c.reject(errorReply(unknownCommand(args)))
		return
	case !cmd.validArity(len(args)):
		c.reject(errorReply("ERR wrong number of arguments for '" + strings.ToLower(string(args[0])) + "' command"))
		return
	case !c.authed && !cmd.noAuth:
		c.reject(errorReply("NOAUTH Authentication required."))
		return
	}
	if c.multi && !cmd.inMulti {
		if cmd.kind == kindConn {
			c.reject(errorReply("ERR Command not allowed inside a transaction"))
			return
		}
		c.queue = append(c.queue, queued{cmd: cmd, args: args})
		c.w.Simple("QUEUED")
		return
	}
	s.processed.Add(1)
	s.run(c, cmd, args).writeTo(c.w)
}

func (c *client) reject(r errorReply) {
	if c.multi {
		c.dirty = true
	}
	r.writeTo(c.w)
}

func (s *Server) run(c *client, cmd command, args [][]byte) reply {
	if cmd.kind == kindConn {
		return cmd.conn(s, c, args)
	}
	if cmd.kind == kindPure {
		r, err := cmd.tx(nil, args)
		if err != nil {
			return storageError(err)
		}
		return r
	}
	var r reply
	fn := func(tx *bitcask.Tx) error {
		var err error
		r, err = cmd.tx(tx, args)
		return err
	}
	var kb [8]string
	scope := bitcask.Keys(cmd.keys.extract(args, kb[:0])...)
	if cmd.global {
		scope = bitcask.Shardwise()
	}
	var err error
	if cmd.kind == kindRead {
		err = s.db.View(scope, fn)
	} else {
		err = s.update(scope, fn)
	}
	if err != nil {
		return storageError(err)
	}
	return r
}

func (s *Server) update(scope bitcask.Scope, fn func(tx *bitcask.Tx) error) error {
	if s.cfg.Replica != nil {
		return s.cfg.Replica.Update(scope, fn)
	}
	return s.db.Update(scope, fn)
}

func (s *Server) flush() error {
	if s.cfg.Replica != nil {
		return s.cfg.Replica.Flush()
	}
	return s.db.Flush()
}
