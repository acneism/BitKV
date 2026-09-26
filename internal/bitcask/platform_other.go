//go:build !windows && !linux && !darwin && !freebsd && !netbsd && !openbsd && !dragonfly

package bitcask

import "os"

type fileLock struct {
	f *os.File
}

func lockFile(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) unlock() error {
	return l.f.Close()
}

func syncDir(string) error {
	return nil
}

func openReaders(string) ([]*os.File, error) {
	return nil, nil
}

func readFull(f *os.File, b []byte, off int64) error {
	_, err := f.ReadAt(b, off)
	return err
}

func writeFull(f *os.File, b []byte, off int64) error {
	_, err := f.WriteAt(b, off)
	return err
}

func mmapFile(*os.File, int64) ([]byte, error) {
	return nil, nil
}

func munmap([]byte) error {
	return nil
}
