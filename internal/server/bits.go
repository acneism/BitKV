package server

import (
	"math"
	"math/bits"

	"github.com/acneism/casketdb/internal/bitcask"
)

var bitCommands = map[string]command{
	"setbit":      {arity: 4, kind: kindWrite, keys: oneKey, acl: catBitmap, tx: cmdSetBit},
	"getbit":      {arity: 3, kind: kindRead, keys: oneKey, acl: catBitmap | catFast, tx: cmdGetBit},
	"bitcount":    {arity: -2, kind: kindRead, keys: oneKey, acl: catBitmap, tx: cmdBitCount},
	"bitpos":      {arity: -3, kind: kindRead, keys: oneKey, acl: catBitmap, tx: cmdBitPos},
	"bitop":       {arity: -4, kind: kindWrite, keys: keySpec{first: 2, last: -1, step: 1}, acl: catBitmap, tx: cmdBitOp},
	"bitfield":    {arity: -2, kind: kindWrite, keys: oneKey, acl: catBitmap, tx: cmdBitField},
	"bitfield_ro": {arity: -2, kind: kindRead, keys: oneKey, acl: catBitmap | catFast, tx: cmdBitFieldRO},
}

const (
	errBitOffset = "ERR bit offset is not an integer or out of range"
	errBitType   = "ERR Invalid bitfield type. Use something like i16 u8. Note that u64 is not supported but i64 is."
)

func bitOffset(arg []byte, width int) (int64, bool) {
	hash := width > 0 && len(arg) > 0 && arg[0] == '#'
	if hash {
		arg = arg[1:]
	}
	n, ok := parseInt(arg)
	if !ok {
		return 0, false
	}
	if hash {
		if n > math.MaxInt64/int64(width) || n < math.MinInt64/int64(width) {
			return 0, false
		}
		n *= int64(width)
	}
	return n, n >= 0 && n>>3 < maxString
}

func bitAt(p []byte, i int64) uint64 {
	if i>>3 >= int64(len(p)) {
		return 0
	}
	return uint64(p[i>>3]>>(7-i&7)) & 1
}

func grownCopy(p []byte, size int64) []byte {
	buf := make([]byte, max(int64(len(p)), size))
	copy(buf, p)
	return buf
}

func cmdSetBit(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	offset, ok := bitOffset(args[2], 0)
	if !ok {
		return errorReply(errBitOffset), nil
	}
	on, ok := parseInt(args[3])
	if !ok || (on != 0 && on != 1) {
		return errorReply("ERR bit is not an integer or out of range"), nil
	}
	value, found, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	old := bitAt(value, offset)
	if found && offset>>3 < int64(len(value)) && old == uint64(on) {
		return intReply(old), nil
	}
	buf := grownCopy(value, offset>>3+1)
	shift := 7 - offset&7
	buf[offset>>3] = buf[offset>>3]&^(1<<shift) | byte(on)<<shift
	expireAt, _ := tx.ExpireAt(key)
	tx.Put(key, buf, expireAt)
	return intReply(old), nil
}

func cmdGetBit(tx *bitcask.Tx, args [][]byte) (reply, error) {
	offset, ok := bitOffset(args[2], 0)
	if !ok {
		return errorReply(errBitOffset), nil
	}
	value, _, err := tx.Get(string(args[1]))
	if err != nil {
		return readError(err)
	}
	return intReply(bitAt(value, offset)), nil
}

func bitRange(args [][]byte, n int64, end *int64) (int64, int64, reply) {
	start, ok := parseInt(args[0])
	if !ok {
		return 0, 0, errorReply(errNotInteger)
	}
	if len(args) >= 2 {
		if *end, ok = parseInt(args[1]); !ok {
			return 0, 0, errorReply(errNotInteger)
		}
	}
	unit := int64(8)
	if len(args) == 3 {
		switch upper(args[2]) {
		case "BIT":
			unit = 1
		case "BYTE":
		default:
			return 0, 0, errorReply(errSyntax)
		}
	}
	last := n*8/unit - 1
	if len(args) >= 2 {
		last = *end
	}
	from, to := byteRange(n*8/unit, start, last)
	return from * unit, to*unit - 1, nil
}

func cmdBitCount(tx *bitcask.Tx, args [][]byte) (reply, error) {
	value, _, err := tx.Get(string(args[1]))
	if err != nil {
		return readError(err)
	}
	lo, hi := int64(0), int64(len(value))*8-1
	switch len(args) {
	case 2:
	case 4, 5:
		var end int64
		var bad reply
		if lo, hi, bad = bitRange(args[2:], int64(len(value)), &end); bad != nil {
			return bad, nil
		}
		if start, _ := parseInt(args[2]); start < 0 && end < 0 && start > end {
			return intReply(0), nil
		}
	default:
		return errorReply(errSyntax), nil
	}
	var count int
	for i := lo; i <= hi; {
		if i&7 == 0 && i+7 <= hi {
			count += bits.OnesCount8(value[i>>3])
			i += 8
			continue
		}
		count += int(bitAt(value, i))
		i++
	}
	return intReply(count), nil
}

func cmdBitPos(tx *bitcask.Tx, args [][]byte) (reply, error) {
	bit, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	if bit != 0 && bit != 1 {
		return errorReply("ERR The bit argument must be 1 or 0."), nil
	}
	value, found, err := tx.Get(string(args[1]))
	if err != nil {
		return readError(err)
	}
	if !found {
		return intReply(-bit), nil
	}
	lo, hi := int64(0), int64(len(value))*8-1
	end := int64(-1)
	endGiven := len(args) >= 5
	switch len(args) {
	case 3:
	case 4, 5, 6:
		var bad reply
		if lo, hi, bad = bitRange(args[3:], int64(len(value)), &end); bad != nil {
			return bad, nil
		}
	default:
		return errorReply(errSyntax), nil
	}
	if lo > hi {
		return intReply(-1), nil
	}
	skip := byte(0)
	if bit == 0 {
		skip = 0xFF
	}
	for i := lo; i <= hi; {
		if i&7 == 0 && i+7 <= hi && value[i>>3] == skip {
			i += 8
			continue
		}
		if bitAt(value, i) == uint64(bit) {
			return intReply(i), nil
		}
		i++
	}
	if bit == 0 && !endGiven {
		return intReply(hi + 1), nil
	}
	return intReply(-1), nil
}

func cmdBitOp(tx *bitcask.Tx, args [][]byte) (reply, error) {
	op, dest, srcs := upper(args[1]), string(args[2]), args[3:]
	switch op {
	case "AND", "OR", "XOR":
	case "NOT":
		if len(srcs) != 1 {
			return errorReply("ERR BITOP NOT must be called with a single source key."), nil
		}
	default:
		return errorReply(errSyntax), nil
	}
	values := make([][]byte, len(srcs))
	size := 0
	for i, key := range srcs {
		v, _, err := tx.Get(string(key))
		if err != nil {
			return readError(err)
		}
		values[i], size = v, max(size, len(v))
	}
	if size == 0 {
		tx.Delete(dest)
		return intReply(0), nil
	}
	out := make([]byte, size)
	for j := range out {
		var x byte
		for i, v := range values {
			var b byte
			if j < len(v) {
				b = v[j]
			}
			switch {
			case i == 0:
				x = b
			case op == "AND":
				x &= b
			case op == "OR":
				x |= b
			default:
				x ^= b
			}
		}
		if op == "NOT" {
			x = ^x
		}
		out[j] = x
	}
	tx.Put(dest, out, 0)
	return intReply(size), nil
}

type fieldOp struct {
	op       byte
	signed   bool
	width    int
	offset   int64
	value    int64
	overflow byte
}

func parseFieldOps(args [][]byte) ([]fieldOp, reply) {
	var ops []fieldOp
	overflow := byte('W')
	for i := 2; i < len(args); i++ {
		sub, left := upper(args[i]), len(args)-1-i
		switch {
		case sub == "GET" && left >= 2, (sub == "SET" || sub == "INCRBY") && left >= 3:
		case sub == "OVERFLOW" && left >= 1:
			switch mode := upper(args[i+1]); mode {
			case "WRAP", "SAT", "FAIL":
				overflow = mode[0]
			default:
				return nil, errorReply("ERR Invalid OVERFLOW type specified")
			}
			i++
			continue
		default:
			return nil, errorReply(errSyntax)
		}
		t := args[i+1]
		width, ok := parseInt(t[min(1, len(t)):])
		signed := len(t) > 0 && (t[0] == 'i' || t[0] == 'I')
		unsigned := len(t) > 0 && (t[0] == 'u' || t[0] == 'U')
		if !ok || width < 1 || (signed && width > 64) || (unsigned && width > 63) || (!signed && !unsigned) {
			return nil, errorReply(errBitType)
		}
		offset, ok := bitOffset(args[i+2], int(width))
		if !ok {
			return nil, errorReply(errBitOffset)
		}
		f := fieldOp{op: sub[0], signed: signed, width: int(width), offset: offset, overflow: overflow}
		if sub != "GET" {
			if f.value, ok = parseInt(args[i+3]); !ok {
				return nil, errorReply(errNotInteger)
			}
			i++
		}
		ops = append(ops, f)
		i += 2
	}
	return ops, nil
}

func getField(p []byte, offset int64, width int) uint64 {
	var v uint64
	for i := range int64(width) {
		v = v<<1 | bitAt(p, offset+i)
	}
	return v
}

func setField(p []byte, offset int64, width int, v uint64) {
	for i := range int64(width) {
		pos := offset + i
		shift := 7 - pos&7
		p[pos>>3] = p[pos>>3]&^(1<<shift) | byte(v>>(int64(width)-1-i)&1)<<shift
	}
}

func signExtend(v uint64, width int) int64 {
	if width == 64 {
		return int64(v)
	}
	v &= 1<<width - 1
	if v&(1<<(width-1)) != 0 {
		v |= ^uint64(0) << width
	}
	return int64(v)
}

func unsignedOverflow(value uint64, incr int64, width int, mode byte) (uint64, bool) {
	limit := uint64(1)<<width - 1
	maxIncr, minIncr := int64(limit-value), -int64(value)
	var saturated uint64
	switch {
	case value > limit || (incr > 0 && incr > maxIncr):
		saturated = limit
	case incr < 0 && incr < minIncr:
	default:
		return value + uint64(incr), false
	}
	if mode == 'W' {
		return (value + uint64(incr)) & limit, true
	}
	return saturated, true
}

func signedOverflow(value, incr int64, width int, mode byte) (int64, bool) {
	limit := int64(math.MaxInt64)
	if width < 64 {
		limit = int64(1)<<(width-1) - 1
	}
	low := -limit - 1
	maxIncr, minIncr := int64(uint64(limit)-uint64(value)), low-value
	var saturated int64
	switch {
	case value > limit || (width != 64 && incr > maxIncr) || (value >= 0 && incr > 0 && incr > maxIncr):
		saturated = limit
	case value < low || (width != 64 && incr < minIncr) || (value < 0 && incr < 0 && incr < minIncr):
		saturated = low
	default:
		return value + incr, false
	}
	if mode == 'W' {
		return signExtend(uint64(value)+uint64(incr), width), true
	}
	return saturated, true
}

func bitfield(tx *bitcask.Tx, args [][]byte, readOnly bool) (reply, error) {
	key := string(args[1])
	ops, bad := parseFieldOps(args)
	if bad != nil {
		return bad, nil
	}
	highest := int64(-1)
	for _, f := range ops {
		if f.op != 'G' {
			highest = max(highest, f.offset+int64(f.width)-1)
		}
	}
	if readOnly && highest >= 0 {
		return errorReply("ERR BITFIELD_RO only supports the GET subcommand"), nil
	}
	value, found, err := tx.Get(key)
	if err != nil {
		return readError(err)
	}
	changed := false
	if highest >= 0 {
		changed = !found || highest>>3 >= int64(len(value))
		value = grownCopy(value, highest>>3+1)
	}
	out := make(arrayReply, 0, len(ops))
	for _, f := range ops {
		old := getField(value, f.offset, f.width)
		if f.op == 'G' {
			if f.signed {
				out = append(out, intReply(signExtend(old, f.width)))
			} else {
				out = append(out, intReply(old))
			}
			continue
		}
		var next uint64
		var result int64
		var overflow bool
		if f.signed {
			var v int64
			if f.op == 'I' {
				v, overflow = signedOverflow(signExtend(old, f.width), f.value, f.width, f.overflow)
				result = v
			} else {
				v, overflow = signedOverflow(f.value, 0, f.width, f.overflow)
				result = signExtend(old, f.width)
			}
			next = uint64(v)
		} else {
			if f.op == 'I' {
				next, overflow = unsignedOverflow(old, f.value, f.width, f.overflow)
				result = int64(next)
			} else {
				next, overflow = unsignedOverflow(uint64(f.value), 0, f.width, f.overflow)
				result = int64(old)
			}
		}
		if overflow && f.overflow == 'F' {
			out = append(out, nilReply)
			continue
		}
		out = append(out, intReply(result))
		setField(value, f.offset, f.width, next)
		changed = changed || getField(value, f.offset, f.width) != old
	}
	if changed {
		expireAt, _ := tx.ExpireAt(key)
		tx.Put(key, value, expireAt)
	}
	return out, nil
}

func cmdBitField(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return bitfield(tx, args, false)
}

func cmdBitFieldRO(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return bitfield(tx, args, true)
}
