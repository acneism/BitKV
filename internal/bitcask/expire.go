package bitcask

import "time"

const (
	expireShardsPerCycle = 16
	expireCycleBudget    = time.Millisecond
)

func (db *DB) expireCycle() {
	start := time.Now()
	scanned, checked, expired := 0, 0, 0
	for range numShards {
		c, e := db.expireShard()
		if c > 0 {
			scanned++
		}
		checked += c
		expired += e
		if time.Since(start) > expireCycleBudget {
			return
		}
		if scanned >= expireShardsPerCycle && expired*4 <= checked {
			return
		}
	}
}

func (db *DB) expireShard() (checked, expired int) {
	i := db.expireCursor
	db.expireCursor = (i + 1) % numShards
	if db.closed.Load() {
		return 0, 0
	}
	s := &db.kd.shards[i]
	s.mu.RLock()
	if s.ttl == 0 {
		s.mu.RUnlock()
		return 0, 0
	}
	now := db.nowMs()
	var victims []string
	for key, e := range s.m {
		if e.expireAt == 0 {
			continue
		}
		checked++
		if e.expired(now) {
			victims = append(victims, key)
		}
	}
	s.mu.RUnlock()
	if len(victims) == 0 {
		return checked, 0
	}
	s.mu.Lock()
	now = db.nowMs()
	for _, key := range victims {
		if e, ok := s.m[key]; ok && e.expired(now) {
			s.remove(key)
			expired++
		}
	}
	s.mu.Unlock()
	db.expiredKeys.Add(int64(expired))
	return checked, expired
}
