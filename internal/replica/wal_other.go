//go:build !windows

package replica

import (
	"os"

	"github.com/hashicorp/raft-wal/fs"
	"github.com/hashicorp/raft-wal/types"
)

func newWalFS() types.VFS {
	return fs.New()
}

func initWalMeta(string) error {
	return nil
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	if cerr := d.Close(); err == nil {
		err = cerr
	}
	return err
}
