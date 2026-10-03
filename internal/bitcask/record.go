package bitcask

import (
	"bufio"
	"encoding/binary"
	"errors"
	"hash/crc32"
	"io"
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
	flagTyped     byte = 64
	flagMember    byte = 128

	hintMemberBit = 1 << 31
)

type Kind byte

const (
	Table   Kind = 0x80
	Ordered Kind = 0x40
)

type memberRef struct {
	key    string
	gen    uint64
	member string
}

func (r memberRef) recordKey() string {
	b := binary.AppendUvarint(make([]byte, 0, binary.MaxVarintLen64+len(r.key)+8+len(r.member)), uint64(len(r.key)))
	b = append(b, r.key...)
	b = binary.LittleEndian.AppendUint64(b, r.gen)
	return string(append(b, r.member...))
}

func parseMemberKey(k []byte) (memberRef, bool) {
	n, w := binary.Uvarint(k)
	if w <= 0 || n > uint64(len(k)-w) || len(k)-w-int(n) < 8 {
		return memberRef{}, false
	}
	rest := k[w+int(n):]
	return memberRef{key: string(k[w : w+int(n)]), gen: binary.LittleEndian.Uint64(rest), member: string(rest[8:])}, true
}

func tableGen(kind Kind, value []byte) (uint64, bool) {
	if kind&Table == 0 || len(value) < 8 {
		return 0, false
	}
	return binary.LittleEndian.Uint64(value), true
}

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

func appendRecord(dst []byte, flags byte, expireAt int64, key string, kind Kind, value []byte) []byte {
	start := len(dst)
	size := len(value)
	if kind != 0 {
		flags |= flagTyped
		size++
	}
	dst = append(dst, make([]byte, headerSize)...)
	h := dst[start:]
	h[4] = flags
	binary.LittleEndian.PutUint64(h[5:], uint64(expireAt))
	binary.LittleEndian.PutUint32(h[13:], uint32(len(key)))
	binary.LittleEndian.PutUint32(h[17:], uint32(size))
	dst = append(dst, key...)
	if kind != 0 {
		dst = append(dst, byte(kind))
	}
	dst = append(dst, value...)
	binary.LittleEndian.PutUint32(dst[start:], crc32.Checksum(dst[start+4:], castagnoli))
	return dst
}

func storedSize(kind Kind, value []byte) int {
	if kind != 0 {
		return len(value) + 1
	}
	return len(value)
}

func splitStored(flags byte, stored []byte) (Kind, []byte) {
	if flags&flagTyped == 0 || len(stored) == 0 {
		return 0, stored
	}
	return Kind(stored[0]), stored[1:]
}

func appendTxHeader(dst []byte, txid uint64, parts uint32) []byte {
	var v [txHeaderSize]byte
	binary.LittleEndian.PutUint64(v[0:], txid)
	binary.LittleEndian.PutUint32(v[8:], parts)
	return appendRecord(dst, flagTx|flagMore, 0, "", 0, v[:])
}

func decodeTxHeader(r rawRecord) (uint64, uint32, bool) {
	if r.flags&flagTx == 0 || len(r.value) != txHeaderSize {
		return 0, 0, false
	}
	return binary.LittleEndian.Uint64(r.value), binary.LittleEndian.Uint32(r.value[8:]), true
}

func appendControl(dst []byte, flag byte, v uint64) []byte {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	return appendRecord(dst, flag, 0, "", 0, b[:])
}

func decodeControl(r rawRecord, flag byte) (uint64, bool) {
	if r.flags&flag == 0 || len(r.value) != 8 {
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

func appendHint(dst []byte, expireAt, offset int64, key string, valueLen uint32, member bool) []byte {
	start := len(dst)
	dst = append(dst, make([]byte, hintHeaderSize)...)
	h := dst[start:]
	keyLen := uint32(len(key))
	if member {
		keyLen |= hintMemberBit
	}
	binary.LittleEndian.PutUint64(h[4:], uint64(expireAt))
	binary.LittleEndian.PutUint64(h[12:], uint64(offset))
	binary.LittleEndian.PutUint32(h[20:], keyLen)
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

func newScanner(f io.ReaderAt, size int64) *scanner {
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
