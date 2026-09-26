//go:build linux || darwin || freebsd || netbsd || openbsd || dragonfly

package bitcask

import (
	"errors"
	"os"
	"syscall"
)

type fileLock struct {
	f *os.File
}

func lockFile(path string) (*fileLock, error) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, ErrLocked
		}
		return nil, err
	}
	return &fileLock{f: f}, nil
}

func (l *fileLock) unlock() error {
	return l.f.Close()
}

func syncDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer d.Close()
	return d.Sync()
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

func mmapFile(f *os.File, size int64) ([]byte, error) {
	if size <= 0 || int64(int(size)) != size {
		return nil, nil
	}
	return syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
}

func munmap(b []byte) error {
	return syscall.Munmap(b)
}
