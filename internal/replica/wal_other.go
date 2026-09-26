//go:build !windows

package replica

import (
	"github.com/hashicorp/raft-wal/fs"
	"github.com/hashicorp/raft-wal/types"
)

func newWalFS() types.VFS {
	return fs.New()
}

func initWalMeta(string) error {
	return nil
}
