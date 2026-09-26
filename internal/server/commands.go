package server

import (
	"errors"
	"fmt"
	"os"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"time"

	"bitkv/internal/bitcask"
)

const (
	errSyntax     = "ERR syntax error"
	errNotInteger = "ERR value is not an integer or out of range"
	errOverflow   = "ERR increment or decrement would overflow"
	errWrongPass  = "WRONGPASS invalid username-password pair or user is disabled."
	errBadName    = "ERR Client names cannot contain spaces, newlines or special characters."
)

type kind int

const (
	kindConn kind = iota
	kindPure
	kindRead
	kindWrite
)

type txFunc func(tx *bitcask.Tx, args [][]byte) (reply, error)

type connFunc func(s *Server, c *client, args [][]byte) reply

type keySpec struct {
	first, last, step int
}

var (
	oneKey  = keySpec{1, 1, 1}
	allArgs = keySpec{1, -1, 1}
	pairs   = keySpec{1, -1, 2}
)

func (k keySpec) extract(args [][]byte, dst []string) []string {
	if k.step == 0 {
		return dst
	}
	last := k.last
	if last < 0 {
		last += len(args)
	}
	for i := k.first; i <= last && i < len(args); i += k.step {
		dst = append(dst, string(args[i]))
	}
	return dst
}

type command struct {
	arity   int
	kind    kind
	keys    keySpec
	global  bool
	tx      txFunc
	conn    connFunc
	inMulti bool
	noAuth  bool
}

var commands map[string]command

func init() {
	commands = map[string]command{
		"ping":         {arity: -1, kind: kindPure, tx: cmdPing},
		"echo":         {arity: 2, kind: kindPure, tx: cmdEcho},
		"quit":         {arity: -1, kind: kindConn, conn: cmdQuit, inMulti: true, noAuth: true},
		"auth":         {arity: -2, kind: kindConn, conn: cmdAuth, noAuth: true},
		"hello":        {arity: -1, kind: kindConn, conn: cmdHello, noAuth: true},
		"select":       {arity: 2, kind: kindConn, conn: cmdSelect},
		"client":       {arity: -2, kind: kindConn, conn: cmdClient},
		"command":      {arity: -1, kind: kindConn, conn: cmdCommand},
		"config":       {arity: -2, kind: kindConn, conn: cmdConfig},
		"info":         {arity: -1, kind: kindConn, conn: cmdInfo},
		"flushdb":      {arity: -1, kind: kindConn, conn: cmdFlush},
		"flushall":     {arity: -1, kind: kindConn, conn: cmdFlush},
		"save":         {arity: 1, kind: kindConn, conn: cmdSave},
		"bgrewriteaof": {arity: 1, kind: kindConn, conn: cmdBgRewriteAOF},
		"multi":        {arity: 1, kind: kindConn, conn: cmdMulti, inMulti: true},
		"exec":         {arity: 1, kind: kindConn, conn: cmdExec, inMulti: true},
		"discard":      {arity: 1, kind: kindConn, conn: cmdDiscard, inMulti: true},
		"watch":        {arity: -2, kind: kindConn, conn: cmdWatch, inMulti: true},
		"unwatch":      {arity: 1, kind: kindConn, conn: cmdUnwatch, inMulti: true},
		"get":          {arity: 2, kind: kindRead, keys: oneKey, tx: cmdGet},
		"set":          {arity: -3, kind: kindWrite, keys: oneKey, tx: cmdSet},
		"setnx":        {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdSetNX},
		"setex":        {arity: 4, kind: kindWrite, keys: oneKey, tx: cmdSetEX},
		"psetex":       {arity: 4, kind: kindWrite, keys: oneKey, tx: cmdPSetEX},
		"getdel":       {arity: 2, kind: kindWrite, keys: oneKey, tx: cmdGetDel},
		"mget":         {arity: -2, kind: kindRead, keys: allArgs, tx: cmdMGet},
		"mset":         {arity: -3, kind: kindWrite, keys: pairs, tx: cmdMSet},
		"append":       {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdAppend},
		"strlen":       {arity: 2, kind: kindRead, keys: oneKey, tx: cmdStrlen},
		"incr":         {arity: 2, kind: kindWrite, keys: oneKey, tx: cmdIncr},
		"decr":         {arity: 2, kind: kindWrite, keys: oneKey, tx: cmdDecr},
		"incrby":       {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdIncrBy},
		"decrby":       {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdDecrBy},
		"del":          {arity: -2, kind: kindWrite, keys: allArgs, tx: cmdDel},
		"unlink":       {arity: -2, kind: kindWrite, keys: allArgs, tx: cmdDel},
		"exists":       {arity: -2, kind: kindRead, keys: allArgs, tx: cmdExists},
		"type":         {arity: 2, kind: kindRead, keys: oneKey, tx: cmdType},
		"keys":         {arity: 2, kind: kindRead, global: true, tx: cmdKeys},
		"scan":         {arity: -2, kind: kindRead, global: true, tx: cmdScan},
		"dbsize":       {arity: 1, kind: kindRead, global: true, tx: cmdDBSize},
		"expire":       {arity: -3, kind: kindWrite, keys: oneKey, tx: cmdExpire},
		"pexpire":      {arity: -3, kind: kindWrite, keys: oneKey, tx: cmdPExpire},
		"expireat":     {arity: -3, kind: kindWrite, keys: oneKey, tx: cmdExpireAt},
		"pexpireat":    {arity: -3, kind: kindWrite, keys: oneKey, tx: cmdPExpireAt},
		"ttl":          {arity: 2, kind: kindRead, keys: oneKey, tx: cmdTTL},
		"pttl":         {arity: 2, kind: kindRead, keys: oneKey, tx: cmdPTTL},
		"persist":      {arity: 2, kind: kindWrite, keys: oneKey, tx: cmdPersist},
	}
}

func upper(b []byte) string {
	return strings.ToUpper(string(b))
}

func truncate(b []byte, n int) string {
	if len(b) > n {
		b = b[:n]
	}
	return string(b)
}

func parseInt(b []byte) (int64, bool) {
	if len(b) == 0 || len(b) > 20 {
		return 0, false
	}
	digits := b
	if digits[0] == '-' {
		digits = digits[1:]
	}
	if len(digits) == 0 || (digits[0] == '0' && len(b) > 1) {
		return 0, false
	}
	for _, ch := range digits {
		if ch < '0' || ch > '9' {
			return 0, false
		}
	}
	n, err := strconv.ParseInt(string(b), 10, 64)
	return n, err == nil
}

func unknownCommand(args [][]byte) string {
	var b strings.Builder
	b.WriteString("ERR unknown command '")
	b.WriteString(truncate(args[0], 128))
	b.WriteString("', with args beginning with: ")
	for _, a := range args[1:] {
		if b.Len() > 512 {
			break
		}
		b.WriteString("'")
		b.WriteString(truncate(a, 128))
		b.WriteString("' ")
	}
	return b.String()
}

func unknownSubcommand(args [][]byte) errorReply {
	return errorReply(fmt.Sprintf("ERR unknown subcommand or wrong number of arguments for '%s'. Try %s HELP.",
		truncate(args[1], 128), upper(args[0])))
}

func validClientName(b []byte) bool {
	for _, ch := range b {
		if ch < '!' || ch > '~' {
			return false
		}
	}
	return true
}

func cmdPing(_ *bitcask.Tx, args [][]byte) (reply, error) {
	switch len(args) {
	case 1:
		return statusReply("PONG"), nil
	case 2:
		return bulkReply(args[1]), nil
	}
	return errorReply("ERR wrong number of arguments for 'ping' command"), nil
}

func cmdEcho(_ *bitcask.Tx, args [][]byte) (reply, error) {
	return bulkReply(args[1]), nil
}

func cmdQuit(s *Server, c *client, args [][]byte) reply {
	c.quit = true
	return okReply
}

func cmdAuth(s *Server, c *client, args [][]byte) reply {
	if len(args) > 3 {
		return errorReply(errSyntax)
	}
	if len(args) == 2 && s.cfg.RequirePass == "" {
		return errorReply("ERR AUTH <password> called without any password configured for the default user. Are you sure your configuration is correct?")
	}
	user := "default"
	if len(args) == 3 {
		user = string(args[1])
	}
	if !s.checkAuth(user, args[len(args)-1]) {
		return errorReply(errWrongPass)
	}
	c.authed = true
	return okReply
}

func cmdHello(s *Server, c *client, args [][]byte) reply {
	if len(args) >= 2 {
		v, ok := parseInt(args[1])
		if !ok {
			return errorReply("ERR Protocol version is not an integer or out of range")
		}
		if v != 2 {
			return errorReply("NOPROTO unsupported protocol version")
		}
	}
	name, authed := c.name, c.authed
	for i := 2; i < len(args); i++ {
		switch upper(args[i]) {
		case "AUTH":
			if i+2 >= len(args) {
				return errorReply(errSyntax)
			}
			if !s.checkAuth(string(args[i+1]), args[i+2]) {
				return errorReply(errWrongPass)
			}
			authed = true
			i += 2
		case "SETNAME":
			if i+1 >= len(args) {
				return errorReply(errSyntax)
			}
			if !validClientName(args[i+1]) {
				return errorReply(errBadName)
			}
			name = string(args[i+1])
			i++
		default:
			return errorReply(errSyntax)
		}
	}
	if !authed {
		return errorReply("NOAUTH HELLO must be called with the client already authenticated, otherwise the HELLO <proto> AUTH <user> <pass> option can be used to authenticate the client and select the RESP protocol version at the same time")
	}
	c.name, c.authed = name, authed
	return arrayReply{
		bulkReply("server"), bulkReply("redis"),
		bulkReply("version"), bulkReply(redisVersion),
		bulkReply("proto"), intReply(2),
		bulkReply("id"), intReply(c.id),
		bulkReply("mode"), bulkReply("standalone"),
		bulkReply("role"), bulkReply("master"),
		bulkReply("modules"), arrayReply{},
	}
}

func cmdSelect(s *Server, c *client, args [][]byte) reply {
	n, ok := parseInt(args[1])
	if !ok {
		return errorReply(errNotInteger)
	}
	if n != 0 {
		return errorReply("ERR DB index is out of range")
	}
	return okReply
}

func cmdClient(s *Server, c *client, args [][]byte) reply {
	switch sub := upper(args[1]); {
	case sub == "ID" && len(args) == 2:
		return intReply(c.id)
	case sub == "GETNAME" && len(args) == 2:
		if c.name == "" {
			return nilReply
		}
		return bulkReply(c.name)
	case sub == "SETNAME" && len(args) == 3:
		if !validClientName(args[2]) {
			return errorReply(errBadName)
		}
		c.name = string(args[2])
		return okReply
	case sub == "SETINFO" && len(args) == 4:
		return okReply
	}
	return unknownSubcommand(args)
}

func cmdCommand(s *Server, c *client, args [][]byte) reply {
	if len(args) == 1 {
		return arrayReply{}
	}
	switch upper(args[1]) {
	case "COUNT":
		return intReply(len(commands))
	case "DOCS", "INFO", "LIST":
		return arrayReply{}
	}
	return unknownSubcommand(args)
}

func cmdConfig(s *Server, c *client, args [][]byte) reply {
	switch upper(args[1]) {
	case "GET":
		if len(args) < 3 {
			return errorReply("ERR wrong number of arguments for 'config|get' command")
		}
		params := map[string]string{
			"appendonly":         "yes",
			"appendfsync":        s.db.Options().Sync.String(),
			"save":               "",
			"databases":          "1",
			"proto-max-bulk-len": strconv.Itoa(s.cfg.MaxBulkLen),
		}
		names := make([]string, 0, len(params))
		for name := range params {
			names = append(names, name)
		}
		slices.Sort(names)
		var out stringsReply
		for _, name := range names {
			for _, p := range args[2:] {
				if matchGlob(strings.ToLower(string(p)), name) {
					out = append(out, name, params[name])
					break
				}
			}
		}
		return out
	case "SET":
		return errorReply("ERR CONFIG SET is not supported")
	case "RESETSTAT":
		return okReply
	}
	return unknownSubcommand(args)
}

func cmdInfo(s *Server, c *client, args [][]byte) reply {
	st := s.db.Stats()
	var b strings.Builder
	line := func(format string, a ...any) {
		fmt.Fprintf(&b, format, a...)
		b.WriteString("\r\n")
	}
	line("# Server")
	line("redis_version:%s", redisVersion)
	line("bitkv_version:%s", Version)
	line("redis_mode:standalone")
	line("os:%s %s", runtime.GOOS, runtime.GOARCH)
	line("process_id:%d", os.Getpid())
	line("uptime_in_seconds:%d", int64(time.Since(s.started).Seconds()))
	line("")
	line("# Clients")
	line("connected_clients:%d", s.clientCount())
	line("")
	line("# Persistence")
	line("aof_enabled:1")
	line("appendfsync:%s", s.db.Options().Sync)
	line("bitcask_logs:%d", st.Logs)
	line("bitcask_data_files:%d", st.DataFiles)
	line("bitcask_total_bytes:%d", st.TotalBytes)
	line("bitcask_live_bytes:%d", st.LiveBytes)
	line("bitcask_merges:%d", st.Merges)
	line("bitcask_writes:%d", st.Writes)
	line("bitcask_fsyncs:%d", st.Fsyncs)
	line("")
	line("# Stats")
	line("total_connections_received:%d", s.connections.Load())
	line("total_commands_processed:%d", s.processed.Load())
	line("expired_keys:%d", st.ExpiredKeys)
	line("")
	line("# Keyspace")
	if st.Keys > 0 {
		line("db0:keys=%d,expires=%d,avg_ttl=0", st.Keys, st.KeysWithTTL)
	}
	return bulkReply(b.String())
}

func cmdFlush(s *Server, c *client, args [][]byte) reply {
	if len(args) > 2 {
		return errorReply(errSyntax)
	}
	if len(args) == 2 {
		if mode := upper(args[1]); mode != "ASYNC" && mode != "SYNC" {
			return errorReply(errSyntax)
		}
	}
	if err := s.db.Flush(); err != nil {
		return storageError(err)
	}
	return okReply
}

func cmdSave(s *Server, c *client, args [][]byte) reply {
	if err := s.db.Sync(); err != nil {
		return storageError(err)
	}
	return okReply
}

func cmdBgRewriteAOF(s *Server, c *client, args [][]byte) reply {
	go func() {
		err := s.db.Merge()
		if err != nil && !errors.Is(err, bitcask.ErrMergeInProgress) && !errors.Is(err, bitcask.ErrClosed) {
			s.log.Error("merge failed", "err", err)
		}
	}()
	return statusReply("Background append only file rewriting started")
}
