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

	"bitkv/internal/bitcask"
	"bitkv/internal/resp"
)

const (
	Version      = "0.4.0"
	redisVersion = "7.2.0"
)

var ErrServerClosed = errors.New("server: closed")

type Config struct {
	MaxBulkLen  int
	RequirePass string
	Logger      *slog.Logger
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
	for !c.quit {
		args, err := c.r.ReadCommand()
		if err != nil {
			var pe *resp.ProtocolError
			if errors.As(err, &pe) {
				c.w.Error("ERR " + pe.Error())
				c.w.Flush()
			}
			return
		}
		if len(args) > 0 {
			s.execute(c, args)
		}
		if c.quit || c.r.Buffered() == 0 {
			if err := c.w.Flush(); err != nil {
				return
			}
		}
	}
}

func (s *Server) execute(c *client, args [][]byte) {
	defer func() {
		if r := recover(); r != nil {
			s.log.Error("command panicked", "cmd", truncate(args[0], 64), "panic", r)
			c.w.Error("ERR internal error")
			c.quit = true
		}
	}()
	name := strings.ToLower(string(args[0]))
	cmd, ok := commands[name]
	switch {
	case !ok:
		c.reject(errorReply(unknownCommand(args)))
		return
	case (cmd.arity > 0 && len(args) != cmd.arity) || (cmd.arity < 0 && len(args) < -cmd.arity):
		c.reject(errorReply("ERR wrong number of arguments for '" + name + "' command"))
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
	scope := bitcask.Keys(cmd.keys.extract(args, nil)...)
	if cmd.global {
		scope = bitcask.Shardwise()
	}
	var err error
	if cmd.kind == kindRead {
		err = s.db.View(scope, fn)
	} else {
		err = s.db.Update(scope, fn)
	}
	if err != nil {
		return storageError(err)
	}
	return r
}
