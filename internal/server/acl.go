package server

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"fmt"
	"maps"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type category uint32

const (
	catKeyspace category = 1 << iota
	catRead
	catWrite
	catString
	catFast
	catSlow
	catAdmin
	catDangerous
	catConnection
	catTransaction
)

var categoryNames = []string{"keyspace", "read", "write", "string", "fast", "slow", "admin", "dangerous", "connection", "transaction"}

func (cmd command) categories() category {
	c := cmd.acl
	switch cmd.kind {
	case kindRead:
		c |= catRead
	case kindWrite:
		c |= catWrite
	}
	if c&catFast == 0 {
		c |= catSlow
	}
	return c
}

func categoryByName(name string) (category, bool) {
	i := slices.Index(categoryNames, strings.ToLower(name))
	if i < 0 {
		return 0, false
	}
	return category(1) << i, true
}

var aclCommands = map[string]command{
	"acl": {arity: -2, kind: kindConn, acl: catAdmin | catDangerous, conn: cmdACL},
}

type user struct {
	name  string
	perms atomic.Pointer[perms]
}

type perms struct {
	enabled   bool
	nopass    bool
	deleted   bool
	passwords [][32]byte
	commands  []bool
	allKeys   bool
	patterns  []string
	all       bool
}

func newPerms() *perms {
	return &perms{commands: make([]bool, len(commands))}
}

func (p *perms) clone() *perms {
	c := *p
	c.passwords = slices.Clone(p.passwords)
	c.commands = slices.Clone(p.commands)
	c.patterns = slices.Clone(p.patterns)
	return &c
}

func (p *perms) setCommands(allowed bool, match func(command) bool) {
	for _, cmd := range commands {
		if match(cmd) {
			p.commands[cmd.id] = allowed
		}
	}
}

func (p *perms) apply(rule string) bool {
	lower := strings.ToLower(rule)
	switch {
	case lower == "on":
		p.enabled = true
	case lower == "off":
		p.enabled = false
	case lower == "nopass":
		p.nopass, p.passwords = true, nil
	case lower == "resetpass":
		p.nopass, p.passwords = false, nil
	case lower == "allkeys" || rule == "~*":
		p.allKeys, p.patterns = true, nil
	case lower == "resetkeys":
		p.allKeys, p.patterns = false, nil
	case lower == "allcommands" || lower == "+@all":
		p.setCommands(true, func(command) bool { return true })
	case lower == "nocommands" || lower == "-@all":
		p.setCommands(false, func(command) bool { return true })
	case lower == "reset":
		*p = *newPerms()
	case strings.HasPrefix(rule, ">"):
		p.addPassword(sha256.Sum256([]byte(rule[1:])))
	case strings.HasPrefix(rule, "<"):
		p.removePassword(sha256.Sum256([]byte(rule[1:])))
	case strings.HasPrefix(rule, "#") || strings.HasPrefix(rule, "!"):
		h, err := hex.DecodeString(rule[1:])
		if err != nil || len(h) != sha256.Size {
			return false
		}
		if rule[0] == '#' {
			p.addPassword([32]byte(h))
		} else {
			p.removePassword([32]byte(h))
		}
	case strings.HasPrefix(rule, "~"):
		if !p.allKeys {
			p.patterns = append(p.patterns, rule[1:])
		}
	case strings.HasPrefix(lower, "+@") || strings.HasPrefix(lower, "-@"):
		cat, ok := categoryByName(lower[2:])
		if !ok {
			return false
		}
		p.setCommands(lower[0] == '+', func(cmd command) bool { return cmd.categories()&cat != 0 })
	case strings.HasPrefix(lower, "+") || strings.HasPrefix(lower, "-"):
		target, ok := commands[lower[1:]]
		if !ok {
			return false
		}
		p.setCommands(lower[0] == '+', func(cmd command) bool { return cmd.id == target.id })
	default:
		return false
	}
	p.all = p.allKeys && !slices.Contains(p.commands, false)
	return true
}

func (p *perms) addPassword(h [32]byte) {
	if !slices.Contains(p.passwords, h) {
		p.passwords = append(p.passwords, h)
	}
	p.nopass = false
}

func (p *perms) removePassword(h [32]byte) {
	p.passwords = slices.DeleteFunc(p.passwords, func(x [32]byte) bool { return x == h })
}

func (p *perms) accepts(pass []byte) bool {
	if p.nopass {
		return true
	}
	h := sha256.Sum256(pass)
	ok := false
	for _, want := range p.passwords {
		ok = subtle.ConstantTimeCompare(h[:], want[:]) == 1 || ok
	}
	return ok
}

func (p *perms) keyAllowed(key string) bool {
	if p.allKeys {
		return true
	}
	for _, pattern := range p.patterns {
		if matchGlob(pattern, key) {
			return true
		}
	}
	return false
}

func (p *perms) describe() []string {
	out := []string{"off"}
	if p.enabled {
		out[0] = "on"
	}
	if p.nopass {
		out = append(out, "nopass")
	}
	for _, h := range p.passwords {
		out = append(out, "#"+hex.EncodeToString(h[:]))
	}
	if keys := p.describeKeys(); keys != "" {
		out = append(out, keys)
	}
	return append(out, p.describeCommands())
}

func (p *perms) describeKeys() string {
	if p.allKeys {
		return "~*"
	}
	keys := make([]string, len(p.patterns))
	for i, pattern := range p.patterns {
		keys[i] = "~" + pattern
	}
	return strings.Join(keys, " ")
}

func (p *perms) describeCommands() string {
	if !slices.Contains(p.commands, false) {
		return "+@all"
	}
	rules := []string{"-@all"}
	for _, name := range slices.Sorted(maps.Keys(commands)) {
		if p.commands[commands[name].id] {
			rules = append(rules, "+"+name)
		}
	}
	return strings.Join(rules, " ")
}

type users struct {
	mu     sync.RWMutex
	byName map[string]*user
}

func newUsers(password string) *users {
	p := newPerms()
	for _, rule := range []string{"on", "allkeys", "allcommands", "nopass"} {
		p.apply(rule)
	}
	if password != "" {
		p.apply(">" + password)
	}
	u := &user{name: "default"}
	u.perms.Store(p)
	return &users{byName: map[string]*user{"default": u}}
}

func (us *users) get(name string) *user {
	us.mu.RLock()
	defer us.mu.RUnlock()
	return us.byName[name]
}

func (us *users) names() []string {
	us.mu.RLock()
	defer us.mu.RUnlock()
	return slices.Sorted(maps.Keys(us.byName))
}

func (us *users) setPassword(password string) {
	u := us.get("default")
	p := u.perms.Load().clone()
	p.apply("resetpass")
	if password == "" {
		p.apply("nopass")
	} else {
		p.apply(">" + password)
	}
	u.perms.Store(p)
}

func (us *users) authenticate(name string, pass []byte) *user {
	u := us.get(name)
	if u == nil {
		return nil
	}
	if p := u.perms.Load(); !p.enabled || !p.accepts(pass) {
		return nil
	}
	return u
}

func (us *users) setUser(name string, rules []string) string {
	us.mu.Lock()
	defer us.mu.Unlock()
	u := us.byName[name]
	p := newPerms()
	if u != nil {
		p = u.perms.Load().clone()
	}
	for _, rule := range rules {
		if !p.apply(rule) {
			return "ERR Error in ACL SETUSER modifier '" + rule + "': Syntax error"
		}
	}
	if u == nil {
		u = &user{name: name}
		us.byName[name] = u
	}
	u.perms.Store(p)
	return ""
}

func (us *users) delete(names []string) (int, bool) {
	us.mu.Lock()
	defer us.mu.Unlock()
	if slices.Contains(names, "default") {
		return 0, false
	}
	deleted := 0
	for _, name := range names {
		if u, ok := us.byName[name]; ok {
			delete(us.byName, name)
			u.perms.Store(&perms{deleted: true, commands: make([]bool, len(commands))})
			deleted++
		}
	}
	return deleted, true
}

func (s *Server) permission(c *client, cmd command, args [][]byte) string {
	p := c.user.perms.Load()
	switch {
	case p.all:
		return ""
	case !p.commands[cmd.id] && !openToEveryone(cmd, args):
		s.aclLog.add("command", cmd.name, c.user.name, c)
		return "NOPERM User " + c.user.name + " has no permissions to run the '" + cmd.name + "' command"
	}
	var kb [8]string
	for _, key := range cmd.keys.extract(args, kb[:0]) {
		if !p.keyAllowed(key) {
			s.aclLog.add("key", key, c.user.name, c)
			return "NOPERM No permissions to access a key"
		}
	}
	return ""
}

const aclLogMax = 128

type denial struct {
	count                                     int64
	reason, context, object, username, client string
	id                                        int64
	created, updated                          time.Time
}

type aclLog struct {
	mu      sync.Mutex
	entries []*denial
	nextID  int64
}

func (l *aclLog) add(reason, object, username string, c *client) {
	now := time.Now()
	context := "toplevel"
	if c.multi {
		context = "multi"
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	for i, d := range l.entries {
		if d.reason == reason && d.context == context && d.object == object && d.username == username && now.Sub(d.updated) < time.Minute {
			d.count++
			d.updated = now
			copy(l.entries[1:i+1], l.entries[:i])
			l.entries[0] = d
			return
		}
	}
	client := fmt.Sprintf("id=%d addr=%s laddr=%s name=%s", c.id, c.conn.RemoteAddr(), c.conn.LocalAddr(), c.name)
	d := &denial{count: 1, reason: reason, context: context, object: object, username: username, client: client, id: l.nextID, created: now, updated: now}
	l.nextID++
	l.entries = append([]*denial{d}, l.entries...)
	if len(l.entries) > aclLogMax {
		l.entries = l.entries[:aclLogMax]
	}
}

func (l *aclLog) reply(n int) arrayReply {
	l.mu.Lock()
	defer l.mu.Unlock()
	now := time.Now()
	out := arrayReply{}
	for _, d := range l.entries[:min(n, len(l.entries))] {
		out = append(out, arrayReply{
			bulkReply("count"), intReply(d.count),
			bulkReply("reason"), bulkReply(d.reason),
			bulkReply("context"), bulkReply(d.context),
			bulkReply("object"), bulkReply(d.object),
			bulkReply("username"), bulkReply(d.username),
			bulkReply("age-seconds"), bulkReply(strconv.FormatFloat(now.Sub(d.created).Seconds(), 'f', 3, 64)),
			bulkReply("client-info"), bulkReply(d.client),
			bulkReply("entry-id"), intReply(d.id),
			bulkReply("timestamp-created"), intReply(d.created.UnixMilli()),
			bulkReply("timestamp-last-updated"), intReply(d.updated.UnixMilli()),
		})
	}
	return out
}

func (l *aclLog) reset() {
	l.mu.Lock()
	l.entries = nil
	l.mu.Unlock()
}

func openToEveryone(cmd command, args [][]byte) bool {
	if cmd.name != "acl" {
		return false
	}
	sub := upper(args[1])
	return sub == "WHOAMI" || sub == "CAT"
}

func cmdACL(s *Server, c *client, args [][]byte) reply {
	switch sub := upper(args[1]); {
	case sub == "WHOAMI" && len(args) == 2:
		return bulkReply(c.user.name)
	case sub == "USERS" && len(args) == 2:
		return stringsReply(s.users.names())
	case sub == "LIST" && len(args) == 2:
		var out stringsReply
		for _, name := range s.users.names() {
			if u := s.users.get(name); u != nil {
				out = append(out, strings.Join(append([]string{"user", name}, u.perms.Load().describe()...), " "))
			}
		}
		return out
	case sub == "GETUSER" && len(args) == 3:
		u := s.users.get(string(args[2]))
		if u == nil {
			return nilReply
		}
		p := u.perms.Load()
		flags := stringsReply{"off"}
		if p.enabled {
			flags[0] = "on"
		}
		if p.nopass {
			flags = append(flags, "nopass")
		}
		passwords := stringsReply{}
		for _, h := range p.passwords {
			passwords = append(passwords, hex.EncodeToString(h[:]))
		}
		return arrayReply{
			bulkReply("flags"), flags,
			bulkReply("passwords"), passwords,
			bulkReply("commands"), bulkReply(p.describeCommands()),
			bulkReply("keys"), bulkReply(p.describeKeys()),
			bulkReply("channels"), bulkReply(""),
			bulkReply("selectors"), arrayReply{},
		}
	case sub == "SETUSER" && len(args) >= 3:
		rules := make([]string, len(args)-3)
		for i, a := range args[3:] {
			rules[i] = string(a)
		}
		if msg := s.users.setUser(string(args[2]), rules); msg != "" {
			return errorReply(msg)
		}
		s.log.Info("ACL user changed", "user", string(args[2]), "by", c.user.name)
		return okReply
	case sub == "DELUSER" && len(args) >= 3:
		names := make([]string, len(args)-2)
		for i, a := range args[2:] {
			names[i] = string(a)
		}
		n, ok := s.users.delete(names)
		if !ok {
			return errorReply("ERR The 'default' user cannot be removed")
		}
		s.log.Info("ACL users deleted", "users", names, "by", c.user.name)
		return intReply(n)
	case sub == "LOG" && len(args) == 2:
		return s.aclLog.reply(10)
	case sub == "LOG" && len(args) == 3 && upper(args[2]) == "RESET":
		s.aclLog.reset()
		return okReply
	case sub == "LOG" && len(args) == 3:
		n, ok := parseInt(args[2])
		if !ok || n < 0 {
			return errorReply(errNotInteger)
		}
		return s.aclLog.reply(int(min(n, aclLogMax)))
	case sub == "CAT" && len(args) == 2:
		return stringsReply(categoryNames)
	case sub == "CAT" && len(args) == 3:
		cat, ok := categoryByName(string(args[2]))
		if !ok {
			return errorReply("ERR Unknown category '" + truncate(args[2], 128) + "'")
		}
		var names stringsReply
		for _, name := range slices.Sorted(maps.Keys(commands)) {
			if commands[name].categories()&cat != 0 {
				names = append(names, name)
			}
		}
		return names
	}
	return unknownSubcommand(args)
}
