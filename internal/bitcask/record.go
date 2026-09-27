package bitcask

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
	"os"
)

const (
	headerSize     = 21
	hintHeaderSize = 28
	maxFieldSize   = 1<<32 - 1

	txHeaderSize = 12

	flagTombstone byte = 1
	flagMore      byte = 2
	flagFlush     byte = 4
	flagTx        byte = 8
	flagTxCommit  byte = 16
	flagMark      byte = 32
)

var (
	castagnoli = crc32.MakeTable(crc32.Castagnoli)
	errTorn    = errors.New("bitcask: torn record")
)

type header struct {
	crc      uint32
	flags    byte
	expireAt int64
	keyLen   uint32
	valueLen uint32
}

type rawRecord struct {
	offset   int64
	flags    byte
	expireAt int64
	key      []byte
	value    []byte
}

func recordSize(keyLen, valueLen int) int64 {
	return int64(headerSize) + int64(keyLen) + int64(valueLen)
}

func appendRecord(dst []byte, flags byte, expireAt int64, key string, value []byte) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, headerSize)...)
	h := dst[start:]
	h[4] = flags
	binary.LittleEndian.PutUint64(h[5:], uint64(expireAt))
	binary.LittleEndian.PutUint32(h[13:], uint32(len(key)))
	binary.LittleEndian.PutUint32(h[17:], uint32(len(value)))
	dst = append(dst, key...)
	dst = append(dst, value...)
	binary.LittleEndian.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], castagnoli))
	return dst
}

func appendTxHeader(dst []byte, txid uint64, parts uint32) []byte {
	var v [txHeaderSize]byte
	binary.LittleEndian.PutUint64(v[0:], txid)
	binary.LittleEndian.PutUint32(v[8:], parts)
	return appendRecord(dst, flagTx|flagMore, 0, "", v[:])
}

func decodeTxHeader(r rawRecord) (uint64, uint32, bool) {
	if r.flags&flagTx == 0 || len(r.value) != txHeaderSize {
		return 0, 0, false
	}
	return binary.LittleEndian.Uint64(r.value), binary.LittleEndian.Uint32(r.value[8:]), true
}

func appendTxCommit(dst []byte, txid uint64) []byte {
	var v [8]byte
	binary.LittleEndian.PutUint64(v[:], txid)
	return appendRecord(dst, flagTxCommit, 0, "", v[:])
}

func decodeTxCommit(r rawRecord) (uint64, bool) {
	if r.flags&flagTxCommit == 0 || len(r.value) != 8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(r.value), true
}

func appendMark(dst []byte, index uint64) []byte {
	var v [8]byte
	binary.LittleEndian.PutUint64(v[:], index)
	return appendRecord(dst, flagMark, 0, "", v[:])
}

func decodeMark(r rawRecord) (uint64, bool) {
	if r.flags&flagMark == 0 || len(r.value) != 8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(r.value), true
}

func decodeHeader(b []byte) header {
	return header{
		crc:      binary.LittleEndian.Uint32(b[0:]),
		flags:    b[4],
		expireAt: int64(binary.LittleEndian.Uint64(b[5:])),
		keyLen:   binary.LittleEndian.Uint32(b[13:]),
		valueLen: binary.LittleEndian.Uint32(b[17:]),
	}
}

func appendHint(dst []byte, expireAt, offset int64, key string, valueLen uint32) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, hintHeaderSize)...)
	h := dst[start:]
	binary.LittleEndian.PutUint64(h[4:], uint64(expireAt))
	binary.LittleEndian.PutUint64(h[12:], uint64(offset))
	binary.LittleEndian.PutUint32(h[20:], uint32(len(key)))
	binary.LittleEndian.PutUint32(h[24:], valueLen)
	dst = append(dst, key...)
	binary.LittleEndian.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], castagnoli))
	return dst
}

type scanner struct {
	r      *bufio.Reader
	offset int64
	size   int64
	hdr    [headerSize]byte
}

func newScanner(f *os.File, size int64) *scanner {
	return &scanner{r: bufio.NewReaderSize(io.NewSectionReader(f, 0, size), 1<<20), size: size}
}

func (s *scanner) next() (rawRecord, error) {
	if s.offset == s.size {
		return rawRecord{}, io.EOF
	}
	if s.size-s.offset < headerSize {
		return rawRecord{}, errTorn
	}
	if _, err := io.ReadFull(s.r, s.hdr[:]); err != nil {
		return rawRecord{}, err
	}
	h := decodeHeader(s.hdr[:])
	total := int64(headerSize) + int64(h.keyLen) + int64(h.valueLen)
	if s.size-s.offset < total {
		return rawRecord{}, errTorn
	}
	body := make([]byte, total-headerSize)
	if _, err := io.ReadFull(s.r, body); err != nil {
		return rawRecord{}, err
	}
	if crc32.Update(crc32.Checksum(s.hdr[4:], castagnoli), castagnoli, body) != h.crc {
		return rawRecord{}, errTorn
	}
	rec := rawRecord{
		offset:   s.offset,
		flags:    h.flags,
		expireAt: h.expireAt,
		key:      body[:h.keyLen],
		value:    body[h.keyLen:],
	}
	s.offset += total
	return rec, nil
}
