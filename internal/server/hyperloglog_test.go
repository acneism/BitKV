package server

import (
	"encoding/binary"
	"fmt"
	"math/rand/v2"
	"testing"
)

func TestMurmur64aVerification(t *testing.T) {
	key := make([]byte, 256)
	hashes := make([]byte, 0, 8*256)
	for i := range 256 {
		key[i] = byte(i)
		hashes = binary.LittleEndian.AppendUint64(hashes, murmur64a(key[:i], uint64(256-i)))
	}
	if got := uint32(murmur64a(hashes, 0)); got != 0x1F0D3804 {
		t.Fatalf("SMHasher verification of MurmurHash64A = %#x, want 0x1f0d3804", got)
	}
}

func TestHLLEncodings(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	sparse := newHLL()
	dense, _ := sparseToDense(newHLL())
	for n := range 20000 {
		elem := fmt.Appendf(nil, "%d-%d", n, rng.Uint64())
		var r1, r2 int
		sparse, r1 = hllAdd(sparse, elem)
		dense, r2 = hllAdd(dense, elem)
		if r1 != r2 || r1 < 0 {
			t.Fatalf("element %d: sparse add = %d, dense add = %d", n, r1, r2)
		}
		if sparse[4] == hllSparse && len(sparse) > hllSparseMax {
			t.Fatalf("a sparse HLL grew to %d bytes", len(sparse))
		}
		if n%1000 != 0 && n != 19999 {
			continue
		}
		var a, b [hllRegisters]uint8
		if !mergeHLL(&a, sparse) || !mergeHLL(&b, dense) || a != b {
			t.Fatalf("element %d: sparse and dense registers differ", n)
		}
		if got := hllCount(&a); float64(abs(got-int64(n+1))) > 0.05*float64(n+1) {
			t.Fatalf("estimate %d for %d elements", got, n+1)
		}
	}
	if sparse[4] != hllDense || len(sparse) != hllDenseSize {
		t.Fatalf("20000 elements left the HLL %d bytes long with encoding %d", len(sparse), sparse[4])
	}
}

func TestHyperLogLog(t *testing.T) {
	srv, db, addr := startServer(t, t.TempDir())
	defer stopServer(t, srv, db)
	c := dial(t, addr)
	c.expect(int64(1), "PFADD", "hll", "a", "b", "c", "d", "e", "f", "g")
	c.expect(int64(7), "PFCOUNT", "hll")
	c.expect(status("string"), "TYPE", "hll")
	c.expect("HYLL", "GETRANGE", "hll", "0", "3")
	c.expect("\x80", "GETRANGE", "hll", "15", "15")
	c.expect(int64(1), "PFADD", "h2", "foo", "bar", "zap")
	c.expect(int64(0), "PFADD", "h2", "zap", "zap", "zap")
	c.expect(int64(0), "PFADD", "h2", "foo", "bar")
	c.expect(int64(3), "PFCOUNT", "h2")
	c.expect(int64(1), "PFADD", "some-other-hll", "1", "2", "3")
	c.expect(int64(6), "PFCOUNT", "h2", "some-other-hll")
	c.expect(int64(1), "PFADD", "hll1", "foo", "bar", "zap", "a")
	c.expect(int64(1), "PFADD", "hll2", "a", "b", "c", "foo")
	c.expect(status("OK"), "PFMERGE", "hll3", "hll1", "hll2")
	c.expect(int64(6), "PFCOUNT", "hll3")
	c.expect(int64(1), "PFADD", "empty")
	c.expect(int64(0), "PFADD", "empty")
	c.expect(int64(0), "PFCOUNT", "empty")
	c.expect(int64(0), "PFCOUNT", "missing")
	c.expect(int64(1), "PFADD", "blank", "")
	c.expect(int64(1), "PFADD", "n", "1", "2", "3", "4", "5")
	c.expect(int64(5), "PFCOUNT", "n")
	c.expect(int64(1), "PFADD", "n", "6", "7", "8", "8", "9", "10")
	c.expect(int64(10), "PFCOUNT", "n")

	c.expect(status("OK"), "SET", "str", "v")
	c.expect(errReply(errNotHLL), "PFADD", "str", "x")
	c.expect(errReply(errNotHLL), "PFCOUNT", "str")
	c.expect(errReply(errNotHLL), "PFMERGE", "hll3", "str")
	c.expect(int64(1), "LPUSH", "list", "x")
	c.expect(errReply(errWrongType), "PFADD", "list", "x")
	c.expect(int64(1), "PFADD", "bad", "a", "b", "c")
	c.do("APPEND", "bad", "hello")
	c.expect(errReply(errBadHLL), "PFCOUNT", "bad")
	c.do("SETRANGE", "bad", "0", "0123")
	c.expect(errReply(errNotHLL), "PFCOUNT", "bad")
	c.expect(int64(1), "PFADD", "enc", "a")
	c.do("SETRANGE", "enc", "4", "x")
	c.expect(errReply(errNotHLL), "PFCOUNT", "enc")
	c.expect(int64(1), "PFADD", "len", "a", "b", "c")
	c.do("SETRANGE", "len", "4", "\x00")
	c.expect(errReply(errNotHLL), "PFCOUNT", "len")

	c.expect(int64(1), "EXPIRE", "hll", "100")
	c.expect(int64(1), "PFADD", "hll", "new")
	c.expect(int64(100), "TTL", "hll")
	var dense []string
	for n := range 3000 {
		dense = append(dense, fmt.Sprint("e", n))
	}
	c.expect(int64(1), append([]string{"PFADD", "big"}, dense...)...)
	c.expect(int64(hllDenseSize), "STRLEN", "big")
	got, _ := c.do("PFCOUNT", "big").(int64)
	if got < 2850 || got > 3150 {
		t.Fatalf("PFCOUNT of 3000 elements = %d", got)
	}
	c.expect(status("OK"), "PFMERGE", "fromdense", "big", "hll")
	c.expect(int64(hllDenseSize), "STRLEN", "fromdense")
	union, _ := c.do("PFCOUNT", "big", "hll").(int64)
	c.expect(union, "PFCOUNT", "fromdense")
	if union <= got {
		t.Fatalf("PFCOUNT big hll = %d, PFCOUNT big = %d", union, got)
	}
}
