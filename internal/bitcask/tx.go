package bitcask

import (
	"errors"
	"slices"
)

var ErrNotLocked = errors.New("bitcask: key outside of the transaction scope")

type Scope struct {
	keys      []string
	all       bool
	shardwise bool
}

func Keys(keys ...string) Scope {
	return Scope{keys: keys}
}

func All() Scope {
	return Scope{all: true}
}

func Shardwise() Scope {
	return Scope{shardwise: true}
}

type pendingOp struct {
	value    []byte
	expireAt int64
	deleted  bool
}

func (op pendingOp) gone(now int64) bool {
	return op.deleted || (op.expireAt != 0 && op.expireAt <= now)
}

type proposedOp struct {
	pendingOp
	term uint64
	id   uint64
}

type Version struct {
	exists bool
	fileID uint32
	offset int64
}

type Tx struct {
	db        *DB
	writable  bool
	now       int64
	all       bool
	shardwise bool
	shards    []int
	shardBuf  [4]int
	pending   map[string]pendingOp
	term      uint64
	depends   uint64
	order     []string
	err       error
}

func (db *DB) begin(scope Scope, writable bool) *Tx {
	tx := &Tx{db: db, writable: writable, all: scope.all, shardwise: scope.shardwise && !writable}
	switch {
	case tx.all:
		for i := range db.kd.shards {
			tx.lock(i)
		}
	case tx.shardwise:
	default:
		tx.shards = tx.shardBuf[:0]
		for _, key := range scope.keys {
			tx.shards = append(tx.shards, shardIndex(key))
		}
		slices.Sort(tx.shards)
		tx.shards = slices.Compact(tx.shards)
		for _, i := range tx.shards {
			tx.lock(i)
		}
	}
	tx.now = db.nowMs()
	return tx
}

func (tx *Tx) lock(i int) {
	if tx.writable {
		tx.db.kd.shards[i].mu.Lock()
	} else {
		tx.db.kd.shards[i].mu.RLock()
	}
}

func (tx *Tx) unlock(i int) {
	if tx.writable {
		tx.db.kd.shards[i].mu.Unlock()
	} else {
		tx.db.kd.shards[i].mu.RUnlock()
	}
}

func (tx *Tx) release() {
	if tx.all {
		for i := range tx.db.kd.shards {
			tx.unlock(i)
		}
		return
	}
	for _, i := range tx.shards {
		tx.unlock(i)
	}
}

func (db *DB) View(scope Scope, fn func(tx *Tx) error) error {
	tx := db.begin(scope, false)
	defer tx.release()
	if db.closed.Load() {
		return ErrClosed
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.err
}

func (db *DB) Update(scope Scope, fn func(tx *Tx) error) error {
	waits, err := db.update(scope, fn)
	if err != nil || len(waits) == 0 {
		return err
	}
	return db.await(waits)
}

type Op struct {
	Key      string
	Value    []byte
	ExpireAt int64
	Delete   bool
}

func (db *DB) Propose(scope Scope, term uint64, fn func(tx *Tx) error, publish func(ops []Op) (uint64, error)) (uint64, error) {
	tx := db.begin(scope, true)
	defer tx.release()
	tx.term = term
	if err := db.stateErr(); err != nil {
		return 0, err
	}
	if err := fn(tx); err != nil {
		return tx.depends, err
	}
	if tx.err != nil || len(tx.order) == 0 {
		return tx.depends, tx.err
	}
	id, err := publish(tx.ops())
	if err != nil {
		return tx.depends, err
	}
	for _, key := range tx.order {
		s := db.kd.shard(key)
		if s.proposed == nil {
			s.proposed = make(map[string]proposedOp)
		}
		s.proposed[key] = proposedOp{pendingOp: tx.pending[key], term: term, id: id}
	}
	return id, nil
}

func (db *DB) DropProposed() {
	for i := range db.kd.shards {
		s := &db.kd.shards[i]
		s.mu.Lock()
		s.proposed = nil
		s.mu.Unlock()
	}
}

func (db *DB) update(scope Scope, fn func(tx *Tx) error) ([]waitPoint, error) {
	tx := db.begin(scope, true)
	defer tx.release()
	if err := db.stateErr(); err != nil {
		return nil, err
	}
	if err := fn(tx); err != nil {
		return nil, err
	}
	if tx.err != nil {
		return nil, tx.err
	}
	return tx.commit()
}

func (db *DB) Apply(ops []Op, upTo uint64) error {
	keys := make([]string, len(ops))
	for i, op := range ops {
		keys[i] = op.Key
	}
	_, err := db.update(Keys(keys...), func(tx *Tx) error {
		for _, op := range ops {
			if op.Delete {
				tx.Delete(op.Key)
			} else {
				tx.Put(op.Key, op.Value, op.ExpireAt)
			}
		}
		if upTo == 0 {
			return nil
		}
		for _, op := range ops {
			s := db.kd.shard(op.Key)
			if p, ok := s.proposed[op.Key]; ok && p.id <= upTo {
				delete(s.proposed, op.Key)
			}
		}
		return nil
	})
	return err
}

func (db *DB) Dump(fn func(op Op) error) error {
	now := db.nowMs()
	for i := range db.kd.shards {
		s := &db.kd.shards[i]
		g := db.groupOfShard(i)
		var ops []Op
		s.mu.RLock()
		for key, e := range s.m {
			if e.expired(now) {
				continue
			}
			v, err := g.readEntry(s, key, e)
			if err != nil {
				s.mu.RUnlock()
				return err
			}
			ops = append(ops, Op{Key: key, Value: v, ExpireAt: e.expireAt})
		}
		s.mu.RUnlock()
		for _, op := range ops {
			if err := fn(op); err != nil {
				return err
			}
		}
	}
	return nil
}

func (tx *Tx) ops() []Op {
	ops := make([]Op, len(tx.order))
	for i, key := range tx.order {
		p := tx.pending[key]
		ops[i] = Op{Key: key, Value: p.value, ExpireAt: p.expireAt, Delete: p.deleted}
	}
	return ops
}

func (tx *Tx) Now() int64 {
	return tx.now
}

func (tx *Tx) shardFor(key string) (int, *shard) {
	i := shardIndex(key)
	if !tx.all {
		if _, ok := slices.BinarySearch(tx.shards, i); !ok {
			tx.err = ErrNotLocked
			return i, nil
		}
	}
	return i, &tx.db.kd.shards[i]
}

func (tx *Tx) Get(key string) ([]byte, bool, error) {
	if op, ok := tx.pending[key]; ok {
		if op.deleted {
			return nil, false, nil
		}
		return op.value, true, nil
	}
	i, s := tx.shardFor(key)
	if s == nil {
		return nil, false, ErrNotLocked
	}
	if op, ok := tx.proposed(s, key); ok {
		if op.gone(tx.now) {
			return nil, false, nil
		}
		return op.value, true, nil
	}
	e, ok := s.m[key]
	if !ok || e.expired(tx.now) {
		return nil, false, nil
	}
	v, err := tx.db.groupOfShard(i).readEntry(s, key, e)
	if err != nil {
		return nil, false, err
	}
	return v, true, nil
}

func (tx *Tx) ExpireAt(key string) (int64, bool) {
	if op, ok := tx.pending[key]; ok {
		if op.deleted {
			return 0, false
		}
		return op.expireAt, true
	}
	_, s := tx.shardFor(key)
	if s == nil {
		return 0, false
	}
	return tx.stored(s, key)
}

func (tx *Tx) stored(s *shard, key string) (int64, bool) {
	if op, ok := tx.proposed(s, key); ok {
		if op.gone(tx.now) {
			return 0, false
		}
		return op.expireAt, true
	}
	e, ok := s.m[key]
	if !ok || e.expired(tx.now) {
		return 0, false
	}
	return e.expireAt, true
}

func (tx *Tx) proposed(s *shard, key string) (proposedOp, bool) {
	if tx.term == 0 {
		return proposedOp{}, false
	}
	op, ok := s.proposed[key]
	if !ok || op.term != tx.term {
		return proposedOp{}, false
	}
	tx.depends = max(tx.depends, op.id)
	return op, true
}

func (tx *Tx) Exists(key string) bool {
	_, ok := tx.ExpireAt(key)
	return ok
}

func (tx *Tx) Version(key string) Version {
	_, s := tx.shardFor(key)
	if s == nil {
		return Version{}
	}
	if op, ok := tx.proposed(s, key); ok {
		return Version{exists: !op.gone(tx.now), offset: int64(op.id)}
	}
	e, ok := s.m[key]
	if !ok || e.expired(tx.now) {
		return Version{}
	}
	return Version{exists: true, fileID: e.fileID, offset: e.offset}
}

func (tx *Tx) global() bool {
	if tx.all || tx.shardwise {
		return true
	}
	tx.err = ErrNotLocked
	return false
}

func (tx *Tx) readShard(i int, fn func(s *shard)) {
	s := &tx.db.kd.shards[i]
	if tx.shardwise {
		s.mu.RLock()
		defer s.mu.RUnlock()
	}
	fn(s)
}

func (tx *Tx) Len() int {
	if !tx.global() {
		return 0
	}
	n := 0
	for i := range tx.db.kd.shards {
		tx.readShard(i, func(s *shard) { n += s.count })
	}
	for _, key := range tx.order {
		_, stored := tx.db.kd.shard(key).m[key]
		deleted := tx.pending[key].deleted
		switch {
		case deleted && stored:
			n--
		case !deleted && !stored:
			n++
		}
	}
	return n
}

func (tx *Tx) Scan(cursor uint64, count int, match func(string) bool) (uint64, []string) {
	if !tx.global() {
		return 0, nil
	}
	if count <= 0 {
		count = 10
	}
	var keys []string
	examined := 0
	i := cursor
	for ; i < numShards && examined < count; i++ {
		tx.readShard(int(i), func(s *shard) {
			for key := range s.m {
				examined++
				visible := false
				if op, ok := tx.pending[key]; ok {
					visible = !op.deleted
				} else {
					_, visible = tx.stored(s, key)
				}
				if visible && (match == nil || match(key)) {
					keys = append(keys, key)
				}
			}
			for _, key := range tx.order {
				if shardIndex(key) != int(i) || tx.pending[key].deleted {
					continue
				}
				if _, ok := s.m[key]; !ok && (match == nil || match(key)) {
					keys = append(keys, key)
				}
			}
		})
	}
	if i >= numShards {
		return 0, keys
	}
	return i, keys
}

func (tx *Tx) Put(key string, value []byte, expireAt int64) {
	if !tx.writable {
		tx.err = ErrReadOnly
		return
	}
	if uint64(len(key)) > maxFieldSize || uint64(len(value)) > maxFieldSize {
		tx.err = ErrTooLarge
		return
	}
	if _, s := tx.shardFor(key); s == nil {
		return
	}
	if expireAt != 0 && expireAt <= tx.now {
		tx.Delete(key)
		return
	}
	tx.stage(key, pendingOp{value: value, expireAt: expireAt})
}

func (tx *Tx) Delete(key string) bool {
	if !tx.writable {
		tx.err = ErrReadOnly
		return false
	}
	if !tx.Exists(key) {
		return false
	}
	tx.stage(key, pendingOp{deleted: true})
	return true
}

func (tx *Tx) stage(key string, op pendingOp) {
	if tx.pending == nil {
		tx.pending = make(map[string]pendingOp)
	}
	if _, ok := tx.pending[key]; !ok {
		tx.order = append(tx.order, key)
	}
	tx.pending[key] = op
}

type groupBatch struct {
	g       *logGroup
	b       *pendingBatch
	keys    []string
	offsets []int64
}

func (tx *Tx) commit() ([]waitPoint, error) {
	db := tx.db
	var parts []*groupBatch
	for _, key := range tx.order {
		if tx.pending[key].deleted {
			s := db.kd.shard(key)
			e, ok := s.m[key]
			if !ok {
				continue
			}
			if e.expired(tx.now) {
				s.remove(key)
				continue
			}
		}
		g := db.groupOfKey(key)
		var gb *groupBatch
		for _, p := range parts {
			if p.g == g {
				gb = p
				break
			}
		}
		if gb == nil {
			gb = &groupBatch{g: g}
			parts = append(parts, gb)
		}
		gb.keys = append(gb.keys, key)
	}
	if len(parts) == 0 {
		return nil, nil
	}
	cross := len(parts) > 1
	var txid uint64
	if cross {
		txid = db.nextTxID()
	}
	for _, gb := range parts {
		gb.encode(tx, cross, txid, uint32(len(parts)))
	}
	if cross {
		db.txGate.RLock()
		defer db.txGate.RUnlock()
	}
	for _, gb := range parts {
		if err := gb.g.reserve(gb.b); err != nil {
			return nil, err
		}
	}
	waits := make([]waitPoint, len(parts))
	for i, gb := range parts {
		gb.apply(tx)
		waits[i] = waitPoint{g: gb.g, seq: gb.b.seq}
	}
	if !cross {
		return waits, nil
	}
	if err := db.await(waits); err != nil {
		return nil, err
	}
	for i, gb := range parts {
		c := &pendingBatch{buf: appendControl(nil, flagTxCommit, txid)}
		if err := gb.g.reserve(c); err != nil {
			return nil, err
		}
		waits[i] = waitPoint{g: gb.g, seq: c.seq}
	}
	return nil, db.await(waits)
}

func (gb *groupBatch) encode(tx *Tx, cross bool, txid uint64, parts uint32) {
	var size int64
	if cross {
		size += recordSize(0, txHeaderSize)
	}
	for _, key := range gb.keys {
		size += recordSize(len(key), len(tx.pending[key].value))
	}
	b := &pendingBatch{buf: make([]byte, 0, size)}
	if cross {
		b.buf = appendTxHeader(b.buf, txid, parts)
	}
	gb.offsets = make([]int64, len(gb.keys))
	for i, key := range gb.keys {
		op := tx.pending[key]
		var flags byte
		if op.deleted {
			flags |= flagTombstone
		} else {
			b.refs = append(b.refs, overlayRef{key: key, offset: int64(len(b.buf))})
		}
		if i < len(gb.keys)-1 {
			flags |= flagMore
		}
		gb.offsets[i] = int64(len(b.buf))
		b.buf = appendRecord(b.buf, flags, op.expireAt, key, op.value)
	}
	gb.b = b
}

func (gb *groupBatch) apply(tx *Tx) {
	b := gb.b
	written := b.df.written.Load() >= b.end()
	for i, key := range gb.keys {
		s := tx.db.kd.shard(key)
		op := tx.pending[key]
		if op.deleted {
			s.remove(key)
			continue
		}
		off := b.off + gb.offsets[i]
		s.set(key, entry{fileID: b.df.id, offset: off, valueSize: uint32(len(op.value)), expireAt: op.expireAt})
		if written {
			continue
		}
		start := gb.offsets[i] + headerSize + int64(len(key))
		end := start + int64(len(op.value))
		s.addOverlay(key, overlayEntry{fileID: b.df.id, offset: off, value: b.buf[start:end:end]})
		if b.df.written.Load() >= b.end() {
			s.dropOverlay(key, b.df.id, off)
		}
	}
}
