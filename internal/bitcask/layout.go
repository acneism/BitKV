package bitcask

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
)

const (
	metaName    = "META"
	flushName   = "FLUSH"
	metaMagic   = "casketdb-meta 2"
	defaultLogs = 4
	dirMode     = 0o700
	fileMode    = 0o600
)

var ErrLayout = errors.New("bitcask: incompatible data directory layout")

func groupDir(root string, i int) string {
	return filepath.Join(root, fmt.Sprintf("log-%03d", i))
}

func (db *DB) layout() ([]string, error) {
	if db.opts.Logs < 0 || db.opts.Logs > numShards {
		return nil, fmt.Errorf("%w: logs must be between 1 and %d", ErrLayout, numShards)
	}
	logs, magic, err := readMeta(db.dir)
	if err != nil {
		return nil, err
	}
	if magic == "" {
		legacy, err := listIDs(db.dir, dataExt)
		if err != nil {
			return nil, err
		}
		if len(legacy) > 0 || fileExists(filepath.Join(db.dir, mergeDirName)) {
			if db.opts.Logs > 1 {
				return nil, fmt.Errorf("%w: %s holds a single-log database, options ask for %d logs", ErrLayout, db.dir, db.opts.Logs)
			}
			return []string{db.dir}, nil
		}
		logs = db.opts.Logs
		if logs == 0 {
			logs = defaultLogs
		}
	} else if db.opts.Logs != 0 && db.opts.Logs != logs {
		return nil, fmt.Errorf("%w: %s was created with %d logs, options ask for %d", ErrLayout, db.dir, logs, db.opts.Logs)
	}
	if magic != metaMagic {
		if err := writeAtomic(db.dir, metaName, []byte(fmt.Sprintf("%s\nlogs %d\n", metaMagic, logs))); err != nil {
			return nil, err
		}
	}
	dirs := make([]string, logs)
	for i := range dirs {
		dirs[i] = groupDir(db.dir, i)
	}
	return dirs, nil
}

func readMeta(root string) (int, string, error) {
	raw, err := os.ReadFile(filepath.Join(root, metaName))
	if errors.Is(err, fs.ErrNotExist) {
		return 0, "", nil
	}
	if err != nil {
		return 0, "", err
	}
	lines := strings.Split(strings.TrimSpace(string(raw)), "\n")
	if len(lines) < 2 || !slices.Contains([]string{metaMagic, "casketdb-meta 1", "bitkv-meta 1"}, lines[0]) {
		return 0, "", fmt.Errorf("%w: unrecognized %s", ErrLayout, metaName)
	}
	logs, err := strconv.Atoi(strings.TrimPrefix(lines[1], "logs "))
	if err != nil || logs < 1 || logs > numShards {
		return 0, "", fmt.Errorf("%w: bad log count in %s", ErrLayout, metaName)
	}
	return logs, lines[0], nil
}

func writeAtomic(dir, name string, data []byte) error {
	tmp := filepath.Join(dir, name+".tmp")
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, fileMode)
	if err != nil {
		return err
	}
	_, err = f.Write(data)
	if err = errors.Join(err, f.Sync(), f.Close()); err != nil {
		return err
	}
	if err := os.Rename(tmp, filepath.Join(dir, name)); err != nil {
		return err
	}
	return syncDir(dir)
}

func removeUpTo(dir string, bound uint32) error {
	for _, ext := range []string{dataExt, hintExt} {
		ids, err := listIDs(dir, ext)
		if err != nil {
			return err
		}
		for _, id := range ids {
			if id > bound {
				continue
			}
			if err := removeFiles(dir, id); err != nil {
				return err
			}
		}
	}
	return syncDir(dir)
}

func writeFlushMarker(root string, bounds []uint32) error {
	var b strings.Builder
	for _, bound := range bounds {
		fmt.Fprintf(&b, "%d\n", bound)
	}
	return writeAtomic(root, flushName, []byte(b.String()))
}

func recoverFlush(root string, dirs []string) error {
	marker := filepath.Join(root, flushName)
	raw, err := os.ReadFile(marker)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	fields := strings.Fields(string(raw))
	if len(fields) != len(dirs) {
		return fmt.Errorf("%w: flush marker lists %d logs, database has %d", ErrCorrupt, len(fields), len(dirs))
	}
	for i, field := range fields {
		bound, err := strconv.ParseUint(field, 10, 32)
		if err != nil {
			return fmt.Errorf("%w: invalid flush marker", ErrCorrupt)
		}
		if err := removeUpTo(dirs[i], uint32(bound)); err != nil {
			return err
		}
	}
	if err := os.Remove(marker); err != nil {
		return err
	}
	return syncDir(root)
}
