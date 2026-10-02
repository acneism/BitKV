package bitcask

import "sync"

const numShards = 1024

type entry struct {
	fileID    uint32
	valueSize uint32
	offset    int64
	expireAt  int64
}

func (e entry) expired(now int64) bool {
	return e.expireAt != 0 && e.expireAt <= now
}

func shardIndex[K string | []byte](key K) int {
	h := uint32(2166136261)
	for i := 0; i < len(key); i++ {
		h ^= uint32(key[i])
		h *= 16777619
	}
	return int(h % numShards)
}

type shard struct {
	mu    sync.RWMutex
	m     map[string]entry
	count int
	ttl   int
	live  int64

	ovMu    sync.Mutex
	overlay map[string]overlayEntry

	proposed map[string]proposedOp

	_ [64]byte
}

type keydir struct {
	shards [numShards]shard
}

func newKeydir() *keydir {
	kd := &keydir{}
	for i := range kd.shards {
		kd.shards[i].m = make(map[string]entry)
		kd.shards[i].overlay = make(map[string]overlayEntry)
	}
	return kd
}

func (kd *keydir) shard(key string) *shard {
	return &kd.shards[shardIndex(key)]
}

func (kd *keydir) reset() {
	for i := range kd.shards {
		kd.shards[i].reset()
	}
}

func (kd *keydir) totals() (count, ttl int, live int64) {
	for i := range kd.shards {
		s := &kd.shards[i]
		s.mu.RLock()
		count += s.count
		ttl += s.ttl
		live += s.live
		s.mu.RUnlock()
	}
	return count, ttl, live
}

func (s *shard) reset() {
	s.m = make(map[string]entry)
	s.count, s.ttl, s.live = 0, 0, 0
}

func (s *shard) set(key string, e entry) {
	if old, ok := s.m[key]; ok {
		s.forget(key, old)
	}
	s.m[key] = e
	s.count++
	s.live += recordSize(len(key), int(e.valueSize))
	if e.expireAt != 0 {
		s.ttl++
	}
}

func (s *shard) forget(key string, e entry) {
	s.count--
	s.live -= recordSize(len(key), int(e.valueSize))
	if e.expireAt != 0 {
		s.ttl--
	}
}

func (s *shard) remove(key string) bool {
	old, ok := s.m[key]
	if !ok {
		return false
	}
	delete(s.m, key)
	s.forget(key, old)
	return true
}

func (s *shard) at(key string, fileID uint32, offset int64) (entry, bool) {
	e, ok := s.m[key]
	return e, ok && e.fileID == fileID && e.offset == offset
}

func (s *shard) relocate(key string, oldFile uint32, oldOffset int64, newFile uint32, newOffset int64) {
	if e, ok := s.at(key, oldFile, oldOffset); ok {
		e.fileID, e.offset = newFile, newOffset
		s.m[key] = e
	}
}

func (s *shard) removeAt(key string, fileID uint32, offset int64) {
	if _, ok := s.at(key, fileID, offset); ok {
		s.remove(key)
	}
}

func (s *shard) addOverlay(key string, o overlayEntry) {
	s.ovMu.Lock()
	s.overlay[key] = o
	s.ovMu.Unlock()
}

func (s *shard) getOverlay(key string, fileID uint32, offset int64) (overlayEntry, bool) {
	s.ovMu.Lock()
	o, ok := s.overlay[key]
	s.ovMu.Unlock()
	if !ok || o.fileID != fileID || o.offset != offset {
		return overlayEntry{}, false
	}
	return o, true
}

func (s *shard) dropOverlay(key string, fileID uint32, offset int64) {
	s.ovMu.Lock()
	if o, ok := s.overlay[key]; ok && o.fileID == fileID && o.offset == offset {
		delete(s.overlay, key)
	}
	s.ovMu.Unlock()
}
