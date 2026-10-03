package bitcask

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"sync"
)

type recPos struct {
	file uint32
	off  int64
}

type undoRec struct {
	key      string
	pos      recPos
	prev     entry
	had      bool
	member   memberRef
	isMember bool
}

type txPart struct {
	parts uint32
	undo  []undoRec
}

type loadState struct {
	g              *logGroup
	now            int64
	parts          map[uint64]*txPart
	commits        map[uint64]bool
	touched        map[string]recPos
	mark           uint64
	members        map[memberRef]entry
	touchedMembers map[memberRef]recPos
}

func (db *DB) load() error {
	states := make([]*loadState, len(db.groups))
	errs := make([]error, len(db.groups))
	var wg sync.WaitGroup
	for i, g := range db.groups {
		wg.Go(func() { states[i], errs[i] = g.load() })
	}
	wg.Wait()
	if err := errors.Join(errs...); err != nil {
		return err
	}
	committed := make(map[uint64]bool)
	for _, st := range states {
		for txid := range st.commits {
			committed[txid] = true
		}
	}
	for _, st := range states {
		var missing []uint64
		for txid, p := range st.parts {
			switch {
			case !committed[txid]:
				st.rollback(p)
			case !st.commits[txid]:
				missing = append(missing, txid)
			}
		}
		if err := st.g.writeCommits(missing); err != nil {
			return err
		}
		st.g.mark, st.g.durable = st.mark, st.mark
	}
	for _, st := range states {
		if err := st.attachMembers(); err != nil {
			return err
		}
	}
	return nil
}

func (st *loadState) attachMembers() error {
	tables := make(map[string]*table)
	for r, e := range st.members {
		s := st.g.db.kd.shard(r.key)
		t, known := tables[r.key]
		if !known {
			if main, ok := s.m[r.key]; ok {
				v, kind, err := st.g.readEntry(s, r.key, main)
				if err != nil {
					return err
				}
				if gen, ok := tableGen(kind, v); ok {
					t = s.ensureTable(r.key, gen, kind&Ordered != 0)
				}
			}
			tables[r.key] = t
		}
		if t == nil || t.gen != r.gen {
			continue
		}
		var value []byte
		if t.order != nil {
			v, err := st.g.readMember(s, r, e)
			if err != nil {
				return err
			}
			value = v
		}
		s.setMember(r, e, value)
	}
	st.members, st.touchedMembers = nil, nil
	return nil
}

func (g *logGroup) writeCommits(txids []uint64) error {
	if len(txids) == 0 {
		return nil
	}
	b := &pendingBatch{}
	for _, txid := range txids {
		b.buf = appendControl(b.buf, flagTxCommit, txid)
	}
	if err := g.reserve(b); err != nil {
		return err
	}
	return g.sync()
}

func (g *logGroup) load() (*loadState, error) {
	st := &loadState{
		g:              g,
		now:            g.db.nowMs(),
		parts:          make(map[uint64]*txPart),
		commits:        make(map[uint64]bool),
		touched:        make(map[string]recPos),
		members:        make(map[memberRef]entry),
		touchedMembers: make(map[memberRef]recPos),
	}
	ids, err := listIDs(g.dir, dataExt)
	if err != nil {
		return nil, err
	}
	var flushedAt uint32
	for i, id := range ids {
		df, err := openDataFile(g.dir, id)
		if err != nil {
			return nil, err
		}
		g.files[id] = df
		g.totalBytes.Add(df.size)
		if fileExists(filepath.Join(g.dir, fileName(id, hintExt))) && st.loadHint(df) == nil {
			continue
		}
		flushed, err := st.loadData(df, i == len(ids)-1)
		if err != nil {
			return nil, err
		}
		if flushed {
			flushedAt = id
		}
	}
	for _, id := range ids {
		if id >= flushedAt {
			break
		}
		g.dropFileLocked(id)
		if err := removeFiles(g.dir, id); err != nil {
			return nil, err
		}
	}
	switch {
	case len(ids) == 0:
		err = g.newActive(1)
	case fileExists(filepath.Join(g.dir, fileName(ids[len(ids)-1], hintExt))):
		err = g.newActive(ids[len(ids)-1] + 1)
	default:
		g.active = g.files[ids[len(ids)-1]]
		err = g.active.startMirror()
	}
	for _, df := range g.files {
		if df != g.active {
			df.seal()
		}
	}
	if err == nil && g.active.size > 0 {
		err = g.active.f.Sync()
	}
	return st, err
}

func (st *loadState) loadData(df *dataFile, last bool) (bool, error) {
	sc := newScanner(df.f, df.size)
	var batch []rawRecord
	var committed int64
	flushed := false
	for {
		rec, err := sc.next()
		if err == io.EOF {
			break
		}
		if err != nil {
			if errors.Is(err, errTorn) || errors.Is(err, io.ErrUnexpectedEOF) {
				break
			}
			return false, err
		}
		batch = append(batch, rec)
		if rec.flags&flagMore != 0 {
			continue
		}
		if txid, parts, ok := decodeTxHeader(batch[0]); ok {
			st.applyPart(df.id, txid, parts, batch[1:])
		} else if txid, ok := decodeControl(batch[0], flagTxCommit); ok && len(batch) == 1 {
			st.commits[txid] = true
		} else if index, ok := decodeControl(batch[0], flagMark); ok && len(batch) == 1 {
			st.mark = max(st.mark, index)
		} else {
			for _, r := range batch {
				if st.apply(df.id, r) {
					flushed = true
				}
			}
		}
		batch = batch[:0]
		committed = sc.offset
	}
	if committed == df.size {
		return flushed, nil
	}
	if !last {
		return false, fmt.Errorf("%w: %s at offset %d", ErrCorrupt, filepath.Join(st.g.dir, fileName(df.id, dataExt)), committed)
	}
	if err := df.f.Truncate(committed); err != nil {
		return false, err
	}
	if err := df.f.Sync(); err != nil {
		return false, err
	}
	st.g.totalBytes.Add(committed - df.size)
	df.size = committed
	df.written.Store(committed)
	return flushed, nil
}

func (st *loadState) loadHint(df *dataFile) error {
	f, err := os.Open(filepath.Join(st.g.dir, fileName(df.id, hintExt)))
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReaderSize(f, 256<<10)
	var h [hintHeaderSize]byte
	for {
		if _, err := io.ReadFull(r, h[:]); err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
		expireAt := int64(binary.LittleEndian.Uint64(h[4:]))
		offset := int64(binary.LittleEndian.Uint64(h[12:]))
		keyLen := binary.LittleEndian.Uint32(h[20:])
		member := keyLen&hintMemberBit != 0
		keyLen &^= hintMemberBit
		valueLen := binary.LittleEndian.Uint32(h[24:])
		if offset < 0 || offset+int64(headerSize)+int64(keyLen)+int64(valueLen) > df.size {
			return ErrCorrupt
		}
		raw := make([]byte, keyLen)
		if _, err := io.ReadFull(r, raw); err != nil {
			return err
		}
		if crc32.Update(crc32.Checksum(h[4:], castagnoli), castagnoli, raw) != binary.LittleEndian.Uint32(h[:]) {
			return ErrCorrupt
		}
		if member {
			if r, ok := parseMemberKey(raw); ok {
				st.members[r] = entry{fileID: df.id, offset: offset, valueSize: valueLen}
			}
			continue
		}
		key := string(raw)
		if _, ok := st.touched[key]; ok {
			st.touched[key] = recPos{df.id, offset}
		}
		s := st.g.db.kd.shard(key)
		if expireAt != 0 && expireAt <= st.now {
			s.remove(key)
			continue
		}
		s.set(key, entry{fileID: df.id, offset: offset, valueSize: valueLen, expireAt: expireAt})
	}
}

func (st *loadState) apply(fileID uint32, r rawRecord) bool {
	if r.flags&flagFlush != 0 {
		groups := len(st.g.db.groups)
		for i := st.g.id; i < numShards; i += groups {
			st.g.db.kd.shards[i].reset()
		}
		clear(st.members)
		return true
	}
	if r.flags&flagMember != 0 {
		st.putMember(fileID, r)
	} else {
		st.put(fileID, r)
	}
	return false
}

func (st *loadState) putMember(fileID uint32, r rawRecord) {
	ref, ok := parseMemberKey(r.key)
	if !ok {
		return
	}
	if _, ok := st.touchedMembers[ref]; ok {
		st.touchedMembers[ref] = recPos{fileID, r.offset}
	}
	if r.flags&flagTombstone != 0 {
		delete(st.members, ref)
		return
	}
	st.members[ref] = entry{fileID: fileID, offset: r.offset, valueSize: uint32(len(r.value))}
}

func (st *loadState) put(fileID uint32, r rawRecord) {
	key := string(r.key)
	if _, ok := st.touched[key]; ok {
		st.touched[key] = recPos{fileID, r.offset}
	}
	s := st.g.db.kd.shard(key)
	if r.flags&flagTombstone != 0 || (r.expireAt != 0 && r.expireAt <= st.now) {
		s.remove(key)
		return
	}
	s.set(key, entry{fileID: fileID, offset: r.offset, valueSize: uint32(len(r.value)), expireAt: r.expireAt})
}

func (st *loadState) applyPart(fileID uint32, txid uint64, parts uint32, recs []rawRecord) {
	p := st.parts[txid]
	if p == nil {
		p = &txPart{parts: parts}
		st.parts[txid] = p
	}
	for _, r := range recs {
		pos := recPos{fileID, r.offset}
		if r.flags&flagMember != 0 {
			ref, ok := parseMemberKey(r.key)
			if !ok {
				continue
			}
			prev, had := st.members[ref]
			st.touchedMembers[ref] = pos
			st.putMember(fileID, r)
			p.undo = append(p.undo, undoRec{pos: pos, prev: prev, had: had, member: ref, isMember: true})
			continue
		}
		key := string(r.key)
		prev, had := st.g.db.kd.shard(key).m[key]
		st.touched[key] = pos
		st.put(fileID, r)
		p.undo = append(p.undo, undoRec{key: key, pos: pos, prev: prev, had: had})
	}
}

func (st *loadState) rollback(p *txPart) {
	for i := len(p.undo) - 1; i >= 0; i-- {
		u := p.undo[i]
		if u.isMember {
			switch {
			case st.touchedMembers[u.member] != u.pos:
			case u.had:
				st.members[u.member] = u.prev
			default:
				delete(st.members, u.member)
			}
			continue
		}
		if st.touched[u.key] != u.pos {
			continue
		}
		s := st.g.db.kd.shard(u.key)
		if u.had {
			s.set(u.key, u.prev)
		} else {
			s.remove(u.key)
		}
	}
}

func recoverMerge(dir string) error {
	mergeDir := filepath.Join(dir, mergeDirName)
	raw, err := os.ReadFile(filepath.Join(mergeDir, mergedMarker))
	if errors.Is(err, fs.ErrNotExist) {
		return os.RemoveAll(mergeDir)
	}
	if err != nil {
		return err
	}
	boundary, err := strconv.ParseUint(string(raw), 10, 32)
	if err != nil {
		return fmt.Errorf("%w: invalid merge marker %q", ErrCorrupt, raw)
	}
	entries, err := os.ReadDir(mergeDir)
	if err != nil {
		return err
	}
	for _, ext := range []string{dataExt, hintExt} {
		for _, de := range entries {
			if _, ok := parseID(de.Name(), ext); !ok {
				continue
			}
			if err := os.Rename(filepath.Join(mergeDir, de.Name()), filepath.Join(dir, de.Name())); err != nil {
				return err
			}
		}
	}
	if err := syncDir(dir); err != nil {
		return err
	}
	if err := removeUpTo(dir, uint32(boundary)); err != nil {
		return err
	}
	return os.RemoveAll(mergeDir)
}

func writeMarker(dir string, boundary uint32) error {
	return writeAtomic(dir, mergedMarker, []byte(strconv.FormatUint(uint64(boundary), 10)))
}
