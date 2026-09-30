package bitcask

import (
	"io"
	"os"
	"runtime"
	"syscall"
	"unsafe"
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
	return transferAt(f, b, off, syscall.ReadFile, io.ErrUnexpectedEOF)
}

func writeFull(f *os.File, b []byte, off int64) error {
	return transferAt(f, b, off, syscall.WriteFile, io.ErrShortWrite)
}

func transferAt(f *os.File, b []byte, off int64, op func(syscall.Handle, []byte, *uint32, *syscall.Overlapped) error, short error) error {
	h := syscall.Handle(f.Fd())
	for len(b) > 0 {
		chunk := b[:min(len(b), maxIOChunk)]
		o := syscall.Overlapped{Offset: uint32(off), OffsetHigh: uint32(off >> 32)}
		var n uint32
		if err := op(h, chunk, &n, &o); err != nil {
			if err == syscall.ERROR_HANDLE_EOF {
				return short
			}
			return err
		}
		if n == 0 {
			return short
		}
		b = b[n:]
		off += int64(n)
	}
	return nil
}

func mmapFile(f *os.File, size int64) ([]byte, error) {
	if size <= 0 {
		return nil, nil
	}
	h, err := syscall.CreateFileMapping(syscall.Handle(f.Fd()), nil, syscall.PAGE_READONLY, uint32(size>>32), uint32(size), nil)
	if err != nil {
		return nil, err
	}
	addr, err := syscall.MapViewOfFile(h, syscall.FILE_MAP_READ, 0, 0, uintptr(size))
	syscall.CloseHandle(h)
	if err != nil {
		return nil, err
	}
	var b []byte
	hdr := (*[3]uintptr)(unsafe.Pointer(&b))
	hdr[0], hdr[1], hdr[2] = addr, uintptr(size), uintptr(size)
	return b, nil
}

func munmap(b []byte) error {
	return syscall.UnmapViewOfFile(uintptr(unsafe.Pointer(unsafe.SliceData(b))))
}
