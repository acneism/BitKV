package server

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/bits"
	"slices"

	"github.com/acneism/casketdb/internal/bitcask"
)

const (
	hllP          = 14
	hllQ          = 64 - hllP
	hllRegisters  = 1 << hllP
	hllBits       = 6
	hllMaxValue   = 1<<hllBits - 1
	hllHeader     = 16
	hllDenseSize  = hllHeader + (hllRegisters*hllBits+7)/8
	hllDense      = 0
	hllSparse     = 1
	hllSparseMax  = 3000
	hllAlphaInf   = 0.721347520444481703680
	sparseValMax  = 32
	sparseValRun  = 4
	sparseZeroRun = 64
	errNotHLL     = "WRONGTYPE Key is not a valid HyperLogLog string value."
	errBadHLL     = "INVALIDOBJ Corrupted HLL object detected"
)

var hyperLogLogCommands = map[string]command{
	"pfadd":   {arity: -2, kind: kindWrite, keys: oneKey, acl: catHyperLogLog | catFast, tx: cmdPFAdd},
	"pfcount": {arity: -2, kind: kindRead, keys: allArgs, acl: catHyperLogLog, tx: cmdPFCount},
	"pfmerge": {arity: -2, kind: kindWrite, keys: allArgs, acl: catHyperLogLog, tx: cmdPFMerge},
}

func newHLL() []byte {
	h := make([]byte, hllHeader, hllHeader+2)
	copy(h, "HYLL")
	h[4] = hllSparse
	return append(h, 0x40|(hllRegisters-1)>>8, (hllRegisters-1)&0xff)
}

func validHLL(h []byte) bool {
	return len(h) >= hllHeader && string(h[:4]) == "HYLL" && h[4] <= hllSparse && (h[4] != hllDense || len(h) == hllDenseSize)
}

func murmur64a(key []byte, seed uint64) uint64 {
	const m = 0xc6a4a7935bd1e995
	h := seed ^ uint64(len(key))*m
	n := len(key) &^ 7
	for i := 0; i < n; i += 8 {
		k := binary.LittleEndian.Uint64(key[i:])
		k *= m
		k ^= k >> 47
		k *= m
		h ^= k
		h *= m
	}
	if tail := key[n:]; len(tail) > 0 {
		for i := len(tail) - 1; i >= 0; i-- {
			h ^= uint64(tail[i]) << (8 * i)
		}
		h *= m
	}
	h ^= h >> 47
	h *= m
	h ^= h >> 47
	return h
}

func hllPattern(elem []byte) (int, uint8) {
	hash := murmur64a(elem, 0xadc83b19)
	index := int(hash & (hllRegisters - 1))
	hash = hash>>hllP | 1<<hllQ
	return index, uint8(bits.TrailingZeros64(hash) + 1)
}

func denseGet(regs []byte, i int) uint8 {
	b, fb := i*hllBits/8, uint(i*hllBits)&7
	v := uint(regs[b]) >> fb
	if b+1 < len(regs) {
		v |= uint(regs[b+1]) << (8 - fb)
	}
	return uint8(v & hllMaxValue)
}

func denseSet(regs []byte, i int, v uint8) bool {
	if denseGet(regs, i) >= v {
		return false
	}
	b, fb := i*hllBits/8, uint(i*hllBits)&7
	regs[b] = regs[b]&^byte(hllMaxValue<<fb) | byte(uint(v)<<fb)
	if b+1 < len(regs) {
		regs[b+1] = regs[b+1]&^byte(hllMaxValue>>(8-fb)) | byte(uint(v)>>(8-fb))
	}
	return true
}

func isSparseZero(op byte) bool  { return op&0xc0 == 0 }
func isSparseXZero(op byte) bool { return op&0xc0 == 0x40 }
func sparseValue(op byte) uint8  { return op>>2&0x1f + 1 }
func sparseRun(op byte) int      { return int(op&3) + 1 }

func sparseVal(value uint8, run int) byte {
	return 0x80 | (value-1)<<2 | byte(run-1)
}

func appendSparseZeros(seq []byte, run int) []byte {
	if run > sparseZeroRun {
		return append(seq, 0x40|byte((run-1)>>8), byte((run-1)&0xff))
	}
	return append(seq, byte(run-1))
}

func sparseSet(h []byte, index int, count uint8) ([]byte, int) {
	if count > sparseValMax {
		return promoteHLL(h, index, count)
	}
	p, first, span, prev := hllHeader, 0, 0, -1
	for p < len(h) {
		oplen := 1
		switch {
		case isSparseZero(h[p]):
			span = int(h[p]&0x3f) + 1
		case h[p]&0x80 != 0:
			span = sparseRun(h[p])
		case p+1 < len(h):
			span, oplen = (int(h[p]&0x3f)<<8|int(h[p+1]))+1, 2
		default:
			return h, -1
		}
		if index <= first+span-1 {
			break
		}
		prev = p
		p += oplen
		first += span
	}
	if span == 0 || p >= len(h) {
		return h, -1
	}
	op, oplen := h[p], 1
	if isSparseXZero(op) {
		oplen = 2
	}
	switch isVal := op&0x80 != 0; {
	case isVal && sparseValue(op) >= count:
		return h, 0
	case isVal && sparseRun(op) == 1, isSparseZero(op) && span == 1:
		h[p] = sparseVal(count, 1)
	default:
		last := first + span - 1
		seq := make([]byte, 0, 5)
		if isVal {
			if index != first {
				seq = append(seq, sparseVal(sparseValue(op), index-first))
			}
			seq = append(seq, sparseVal(count, 1))
			if index != last {
				seq = append(seq, sparseVal(sparseValue(op), last-index))
			}
		} else {
			if index != first {
				seq = appendSparseZeros(seq, index-first)
			}
			seq = append(seq, sparseVal(count, 1))
			if index != last {
				seq = appendSparseZeros(seq, last-index)
			}
		}
		if len(seq) > oplen && len(h)+len(seq)-oplen > hllSparseMax {
			return promoteHLL(h, index, count)
		}
		h = slices.Replace(h, p, p+oplen, seq...)
	}
	p = hllHeader
	if prev >= 0 {
		p = prev
	}
	for scan := 5; p < len(h) && scan > 0; scan-- {
		switch {
		case isSparseXZero(h[p]):
			p += 2
			continue
		case isSparseZero(h[p]):
			p++
			continue
		}
		if p+1 < len(h) && h[p+1]&0x80 != 0 && sparseValue(h[p]) == sparseValue(h[p+1]) {
			if run := sparseRun(h[p]) + sparseRun(h[p+1]); run <= sparseValRun {
				h[p+1] = sparseVal(sparseValue(h[p]), run)
				h = slices.Delete(h, p, p+1)
				continue
			}
		}
		p++
	}
	return h, 1
}

func promoteHLL(h []byte, index int, count uint8) ([]byte, int) {
	d, ok := sparseToDense(h)
	if !ok {
		return h, -1
	}
	denseSet(d[hllHeader:], index, count)
	return d, 1
}

func sparseToDense(h []byte) ([]byte, bool) {
	if h[4] == hllDense {
		return h, true
	}
	d := make([]byte, hllDenseSize)
	copy(d, h[:hllHeader])
	d[4] = hllDense
	idx, ok := walkSparse(h, func(i int, v uint8) { denseSet(d[hllHeader:], i, v) })
	return d, ok && idx == hllRegisters
}

func walkSparse(h []byte, set func(i int, v uint8)) (int, bool) {
	idx := 0
	for p := hllHeader; p < len(h); {
		switch op := h[p]; {
		case isSparseZero(op):
			idx += int(op&0x3f) + 1
			p++
		case isSparseXZero(op):
			if p+1 >= len(h) {
				return idx, false
			}
			idx += (int(op&0x3f)<<8 | int(h[p+1])) + 1
			p += 2
		default:
			run, v := sparseRun(op), sparseValue(op)
			if idx+run > hllRegisters {
				return idx, true
			}
			for range run {
				set(idx, v)
				idx++
			}
			p++
		}
	}
	return idx, true
}

func hllAdd(h []byte, elem []byte) ([]byte, int) {
	index, count := hllPattern(elem)
	if h[4] == hllDense {
		if denseSet(h[hllHeader:], index, count) {
			return h, 1
		}
		return h, 0
	}
	return sparseSet(h, index, count)
}

func mergeHLL(regs *[hllRegisters]uint8, h []byte) bool {
	if h[4] == hllDense {
		for i := range regs {
			regs[i] = max(regs[i], denseGet(h[hllHeader:], i))
		}
		return true
	}
	idx, ok := walkSparse(h, func(i int, v uint8) { regs[i] = max(regs[i], v) })
	return ok && idx == hllRegisters
}

func hllSigma(x float64) float64 {
	if x == 1 {
		return math.Inf(1)
	}
	y, z := 1.0, x
	for {
		x *= x
		prev := z
		z += float64(x * y)
		y += y
		if z == prev {
			return z
		}
	}
}

func hllTau(x float64) float64 {
	if x == 0 || x == 1 {
		return 0
	}
	y, z := 1.0, 1-x
	for {
		x = math.Sqrt(x)
		prev := z
		y *= 0.5
		z -= float64(float64((1-x)*(1-x)) * y)
		if z == prev {
			return z / 3
		}
	}
}

func hllCount(regs *[hllRegisters]uint8) int64 {
	var histogram [64]int
	for _, r := range regs {
		histogram[r]++
	}
	const m = hllRegisters
	z := m * hllTau(float64(m-histogram[hllQ+1])/m)
	for j := hllQ; j >= 1; j-- {
		z = (z + float64(histogram[j])) * 0.5
	}
	z += float64(m * hllSigma(float64(histogram[0])/m))
	return int64(math.Round(hllAlphaInf * m * m / z))
}

func readHLL(tx *bitcask.Tx, key []byte) ([]byte, bool, reply, error) {
	h, found, err := tx.Get(string(key))
	if err != nil {
		bad, err := readError(err)
		return nil, false, bad, err
	}
	if found && !validHLL(h) {
		return nil, false, errorReply(errNotHLL), nil
	}
	return h, found, nil, nil
}

func cmdPFAdd(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, found, bad, err := readHLL(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	updated := !found
	if found {
		h = bytes.Clone(h)
	} else {
		h = newHLL()
	}
	for _, elem := range args[2:] {
		var r int
		switch h, r = hllAdd(h, elem); r {
		case 1:
			updated = true
		case -1:
			return errorReply(errBadHLL), nil
		}
	}
	if !updated {
		return intReply(0), nil
	}
	storeHLL(tx, args[1], h)
	return intReply(1), nil
}

func storeHLL(tx *bitcask.Tx, key, h []byte) {
	h[15] |= 0x80
	expireAt, _ := tx.ExpireAt(string(key))
	tx.Put(string(key), h, expireAt)
}

func cmdPFCount(tx *bitcask.Tx, args [][]byte) (reply, error) {
	var regs [hllRegisters]uint8
	for _, key := range args[1:] {
		h, found, bad, err := readHLL(tx, key)
		if bad != nil || err != nil {
			return bad, err
		}
		if !found {
			continue
		}
		if len(args) == 2 && h[15]&0x80 == 0 {
			return intReply(int64(binary.LittleEndian.Uint64(h[8:hllHeader]))), nil
		}
		if !mergeHLL(&regs, h) {
			return errorReply(errBadHLL), nil
		}
	}
	return intReply(hllCount(&regs)), nil
}

func cmdPFMerge(tx *bitcask.Tx, args [][]byte) (reply, error) {
	var regs [hllRegisters]uint8
	dense := false
	for _, key := range args[1:] {
		h, found, bad, err := readHLL(tx, key)
		if bad != nil || err != nil {
			return bad, err
		}
		if !found {
			continue
		}
		dense = dense || h[4] == hllDense
		if !mergeHLL(&regs, h) {
			return errorReply(errBadHLL), nil
		}
	}
	h, found, _, err := readHLL(tx, args[1])
	if err != nil {
		return nil, err
	}
	if found {
		h = bytes.Clone(h)
	} else {
		h = newHLL()
	}
	if dense {
		h, _ = sparseToDense(h)
	}
	for i, v := range regs {
		switch {
		case v == 0:
		case h[4] == hllDense:
			denseSet(h[hllHeader:], i, v)
		default:
			h, _ = sparseSet(h, i, v)
		}
	}
	storeHLL(tx, args[1], h)
	return statusReply("OK"), nil
}
