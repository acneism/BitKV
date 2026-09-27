package bitcask

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/fs"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
)

const (
	dataExt       = ".data"
	hintExt       = ".hint"
	lockName      = "LOCK"
	mergeDirName  = "merge"
	mergedMarker  = "MERGED"
	mirrorInitial = 64 << 10
)

type dataFile struct {
	id      uint32
	f       *os.File
	size    int64
	written atomic.Int64
	readers []*os.File
	next    atomic.Uint32
	mm      []byte
	mem     atomic.Pointer[[]byte]
}

func fileName(id uint32, ext string) string {
	return fmt.Sprintf("%09d%s", id, ext)
}

func openDataFile(dir string, id uint32) (*dataFile, error) {
	path := filepath.Join(dir, fileName(id, dataExt))
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, fileMode)
	if err != nil {
		return nil, err
	}
	st, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, err
	}
	readers, err := openReaders(path)
	if err != nil {
		f.Close()
		return nil, err
	}
	df := &dataFile{id: id, f: f, size: st.Size(), readers: readers}
	df.written.Store(df.size)
	return df, nil
}

func (df *dataFile) close() error {
	df.mem.Store(nil)
	if df.mm != nil {
		munmap(df.mm)
		df.mm = nil
	}
	err := df.f.Close()
	for _, r := range df.readers {
		r.Close()
	}
	return err
}

func (df *dataFile) reader() *os.File {
	if len(df.readers) == 0 {
		return df.f
	}
	return df.readers[df.next.Add(1)%uint32(len(df.readers))]
}

func (df *dataFile) read(offset int64, key string, valueSize uint32) ([]byte, error) {
	n := recordSize(len(key), int(valueSize))
	end := offset + int64(n)
	buf := df.cached(end)
	cached := buf != nil
	if cached {
		buf = buf[offset:end]
	} else {
		buf = make([]byte, n)
		if err := readFull(df.reader(), buf, offset); err != nil {
			return nil, err
		}
	}
	h := decodeHeader(buf)
	if h.crc != crc32.Checksum(buf[4:], castagnoli) ||
		h.keyLen != uint32(len(key)) ||
		h.valueLen != valueSize ||
		string(buf[headerSize:headerSize+len(key)]) != key {
		return nil, fmt.Errorf("%w: %s at offset %d", ErrCorrupt, fileName(df.id, dataExt), offset)
	}
	value := buf[headerSize+len(key):]
	if cached {
		value = append(make([]byte, 0, len(value)), value...)
	}
	return value, nil
}

func parseID(name, ext string) (uint32, bool) {
	base, ok := strings.CutSuffix(name, ext)
	if !ok {
		return 0, false
	}
	n, err := strconv.ParseUint(base, 10, 32)
	if err != nil || n == 0 {
		return 0, false
	}
	return uint32(n), true
}

func listIDs(dir, ext string) ([]uint32, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	var ids []uint32
	for _, de := range entries {
		if de.IsDir() {
			continue
		}
		if id, ok := parseID(de.Name(), ext); ok {
			ids = append(ids, id)
		}
	}
	slices.Sort(ids)
	return ids, nil
}

func fileExists(path string) bool {
	_, err := os.Stat(path)
	return err == nil
}

func removeFiles(dir string, id uint32) error {
	for _, ext := range []string{dataExt, hintExt} {
		err := os.Remove(filepath.Join(dir, fileName(id, ext)))
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (df *dataFile) cached(end int64) []byte {
	if end <= int64(len(df.mm)) {
		return df.mm
	}
	if p := df.mem.Load(); p != nil && end <= int64(len(*p)) {
		return *p
	}
	return nil
}

func (df *dataFile) seal() {
	df.mem.Store(nil)
	if m, err := mmapFile(df.f, df.size); err == nil {
		df.mm = m
	}
}

func (df *dataFile) startMirror() error {
	m := make([]byte, df.size, max(df.size, mirrorInitial))
	if err := readFull(df.f, m, 0); err != nil {
		return err
	}
	df.mem.Store(&m)
	return nil
}

func (df *dataFile) mirror(b []byte, off int64) {
	p := df.mem.Load()
	if p == nil || int64(len(*p)) != off {
		return
	}
	m := append(*p, b...)
	df.mem.Store(&m)
}
