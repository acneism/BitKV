package replica

import (
	"errors"
	"os"
	"path/filepath"

	"github.com/hashicorp/raft-wal/fs"
	"github.com/hashicorp/raft-wal/metadb"
	"github.com/hashicorp/raft-wal/types"
	"go.etcd.io/bbolt"
)

type walFS struct {
	*fs.FS
}

func newWalFS() types.VFS {
	return walFS{fs.New()}
}

func (walFS) Create(dir, name string, size uint64) (types.WritableFile, error) {
	f, err := os.OpenFile(filepath.Join(dir, name), os.O_CREATE|os.O_EXCL|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	if err := f.Truncate(int64(size)); err != nil {
		f.Close()
		return nil, err
	}
	return f, nil
}

func (walFS) Delete(dir, name string) error {
	return os.Remove(filepath.Join(dir, name))
}

func initWalMeta(dir string) error {
	name := filepath.Join(dir, metadb.FileName)
	if _, err := os.Stat(name); !errors.Is(err, os.ErrNotExist) {
		return err
	}
	tmp := name + ".tmp"
	if err := os.RemoveAll(tmp); err != nil {
		return err
	}
	db, err := bbolt.Open(tmp, 0o644, nil)
	if err != nil {
		return err
	}
	err = db.Update(func(tx *bbolt.Tx) error {
		if _, err := tx.CreateBucket([]byte(metadb.MetaBucket)); err != nil {
			return err
		}
		_, err := tx.CreateBucket([]byte(metadb.StableBucket))
		return err
	})
	if cerr := db.Close(); err == nil {
		err = cerr
	}
	if err != nil {
		return err
	}
	return os.Rename(tmp, name)
}
