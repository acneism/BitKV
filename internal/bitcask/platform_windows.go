package bitcask

import (
	"io"
	"os"
	"runtime"
	"syscall"
)

const (
	errorSharingViolation = syscall.Errno(32)
	maxIOChunk            = 1 << 30
)

type fileLock struct {
	h syscall.Handle
}

func lockFile(path string) (*fileLock, error) {
	p, err := syscall.UTF16PtrFromString(path)
	if err != nil {
		return nil, err
	}
	h, err := syscall.CreateFile(p, syscall.GENERIC_READ|syscall.GENERIC_WRITE, 0, nil, syscall.OPEN_ALWAYS, syscall.FILE_ATTRIBUTE_NORMAL, 0)
	if err != nil {
		if err == errorSharingViolation {
			return nil, ErrLocked
		}
		return nil, err
	}
	return &fileLock{h: h}, nil
}

func (l *fileLock) unlock() error {
	return syscall.CloseHandle(l.h)
}

func syncDir(string) error {
	return nil
}

func openReaders(path string) ([]*os.File, error) {
	n := min(runtime.GOMAXPROCS(0), 8)
	readers := make([]*os.File, 0, n)
	for range n {
		f, err := os.Open(path)
		if err != nil {
			for _, r := range readers {
				r.Close()
			}
			return nil, err
		}
		readers = append(readers, f)
	}
	return readers, nil
}

func readFull(f *os.File, b []byte, off int64) error {
	h := syscall.Handle(f.Fd())
	for len(b) > 0 {
		chunk := b[:min(len(b), maxIOChunk)]
		o := syscall.Overlapped{Offset: uint32(off), OffsetHigh: uint32(off >> 32)}
		var n uint32
		if err := syscall.ReadFile(h, chunk, &n, &o); err != nil {
			if err == syscall.ERROR_HANDLE_EOF {
				return io.ErrUnexpectedEOF
			}
			return err
		}
		if n == 0 {
			return io.ErrUnexpectedEOF
		}
		b = b[n:]
		off += int64(n)
	}
	return nil
}

func writeFull(f *os.File, b []byte, off int64) error {
	h := syscall.Handle(f.Fd())
	for len(b) > 0 {
		chunk := b[:min(len(b), maxIOChunk)]
		o := syscall.Overlapped{Offset: uint32(off), OffsetHigh: uint32(off >> 32)}
		var n uint32
		if err := syscall.WriteFile(h, chunk, &n, &o); err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		b = b[n:]
		off += int64(n)
	}
	return nil
}
