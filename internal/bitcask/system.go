package bitcask

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
)

const systemName = "SYSTEM"

func (db *DB) loadSystem() error {
	b, err := os.ReadFile(filepath.Join(db.dir, systemName))
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	db.system = b
	return err
}

func (db *DB) System() []byte {
	db.sysMu.Lock()
	defer db.sysMu.Unlock()
	return db.system
}

func (db *DB) SetSystem(b []byte) error {
	db.sysMu.Lock()
	defer db.sysMu.Unlock()
	if err := db.stateErr(); err != nil {
		return err
	}
	if len(b) == 0 {
		err := os.Remove(filepath.Join(db.dir, systemName))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
		if err := syncDir(db.dir); err != nil {
			return err
		}
		db.system = nil
	} else {
		if err := writeAtomic(db.dir, systemName, b); err != nil {
			return err
		}
		db.system = bytes.Clone(b)
	}
	if db.onSystem != nil {
		db.onSystem(db.system)
	}
	return nil
}

func (db *DB) WatchSystem(fn func([]byte)) {
	db.sysMu.Lock()
	defer db.sysMu.Unlock()
	db.onSystem = fn
	fn(db.system)
}

func (db *DB) linkSystem(add func(src string, size int64) error) error {
	db.sysMu.Lock()
	defer db.sysMu.Unlock()
	if db.system == nil {
		return nil
	}
	return add(filepath.Join(db.dir, systemName), int64(len(db.system)))
}
