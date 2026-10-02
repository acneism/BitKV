package server

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand/v2"
	"strconv"

	"github.com/acneism/casketdb/internal/bitcask"
)

var hashCommands = map[string]command{
	"hset":         {arity: -4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHSet},
	"hmset":        {arity: -4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHMSet},
	"hsetnx":       {arity: 4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHSetNX},
	"hget":         {arity: 3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHGet},
	"hmget":        {arity: -3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHMGet},
	"hdel":         {arity: -3, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHDel},
	"hlen":         {arity: 2, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHLen},
	"hexists":      {arity: 3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHExists},
	"hstrlen":      {arity: 3, kind: kindRead, keys: oneKey, acl: catHash | catFast, tx: cmdHStrlen},
	"hgetall":      {arity: 2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHGetAll},
	"hkeys":        {arity: 2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHKeys},
	"hvals":        {arity: 2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHVals},
	"hincrby":      {arity: 4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHIncrBy},
	"hincrbyfloat": {arity: 4, kind: kindWrite, keys: oneKey, acl: catHash | catFast, tx: cmdHIncrByFloat},
	"hscan":        {arity: -3, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHScan},
	"hrandfield":   {arity: -2, kind: kindRead, keys: oneKey, acl: catHash, tx: cmdHRandField},
}

const (
	maxRandomCount  = 1 << 24
	maxListpackLen  = 128
	maxListpackItem = 64
)

type hash []byte

func (h hash) next() (field, value []byte, rest hash, ok bool) {
	n, k := binary.Uvarint(h)
	if k <= 0 || n > uint64(len(h)-k) {
		return nil, nil, nil, false
	}
	field, h = h[k:k+int(n)], h[k+int(n):]
	n, k = binary.Uvarint(h)
	if k <= 0 || n > uint64(len(h)-k) {
		return nil, nil, nil, false
	}
	return field, h[k : k+int(n)], h[k+int(n):], true
}

func (h hash) each(fn func(field, value []byte)) {
	for rest := h; ; {
		field, value, next, ok := rest.next()
		if !ok {
			return
		}
		fn(field, value)
		rest = next
	}
}

func (h hash) len() int {
	n := 0
	h.each(func(_, _ []byte) { n++ })
	return n
}

func (h hash) find(field []byte) (start, end int, value []byte, ok bool) {
	for rest := h; ; {
		f, v, next, more := rest.next()
		if !more {
			return len(h), len(h), nil, false
		}
		if bytes.Equal(f, field) {
			return len(h) - len(rest), len(h) - len(next), v, true
		}
		rest = next
	}
}

func (h hash) put(field, value []byte) (hash, bool) {
	start, end, _, found := h.find(field)
	out := make(hash, 0, len(h)-(end-start)+2*binary.MaxVarintLen64+len(field)+len(value))
	out = append(out, h[:start]...)
	out = binary.AppendUvarint(out, uint64(len(field)))
	out = append(out, field...)
	out = binary.AppendUvarint(out, uint64(len(value)))
	out = append(out, value...)
	return append(out, h[end:]...), !found
}

func (h hash) del(field []byte) (hash, bool) {
	start, end, _, found := h.find(field)
	if !found {
		return h, false
	}
	return append(append(make(hash, 0, len(h)-(end-start)), h[:start]...), h[end:]...), true
}

type hashView struct {
	tx    *bitcask.Tx
	key   string
	blob  hash
	table bool
	gen   uint64
	count int
	dirty bool
}

func openHash(tx *bitcask.Tx, key []byte) (*hashView, reply, error) {
	value, kind, found, err := tx.GetKind(string(key))
	if err != nil {
		return nil, nil, err
	}
	h := &hashView{tx: tx, key: string(key)}
	switch {
	case !found:
	case kind == typeHash:
		h.blob = value
	case kind == typeHash|bitcask.Table && len(value) >= 8:
		n, _ := binary.Uvarint(value[8:])
		h.table, h.gen, h.count = true, binary.LittleEndian.Uint64(value), int(n)
	default:
		return nil, errorReply(errWrongType), nil
	}
	return h, nil, nil
}

func (h *hashView) len() int {
	if h.table {
		return h.count
	}
	return h.blob.len()
}

func (h *hashView) get(field []byte) ([]byte, bool, error) {
	if h.table {
		return h.tx.GetMember(h.key, string(field))
	}
	_, _, value, ok := h.blob.find(field)
	return value, ok, nil
}

func (h *hashView) set(field, value []byte) (bool, error) {
	h.dirty = true
	if !h.table {
		var added bool
		h.blob, added = h.blob.put(field, value)
		if len(field) > maxListpackItem || len(value) > maxListpackItem || (added && h.blob.len() > maxListpackLen) {
			h.convert()
		}
		return added, nil
	}
	_, found, err := h.tx.GetMember(h.key, string(field))
	if err != nil {
		return false, err
	}
	h.tx.PutMember(h.key, string(field), value)
	if !found {
		h.count++
	}
	return !found, nil
}

func (h *hashView) del(field []byte) bool {
	var deleted bool
	if h.table {
		if deleted = h.tx.DeleteMember(h.key, string(field)); deleted {
			h.count--
		}
	} else {
		h.blob, deleted = h.blob.del(field)
	}
	h.dirty = h.dirty || deleted
	return deleted
}

func (h *hashView) each(values bool, fn func(field, value []byte)) error {
	if h.table {
		return h.tx.Members(h.key, values, func(field string, value []byte) bool {
			fn([]byte(field), value)
			return true
		})
	}
	h.blob.each(fn)
	return nil
}

func (h *hashView) meta() []byte {
	return binary.AppendUvarint(binary.LittleEndian.AppendUint64(nil, h.gen), uint64(h.count))
}

func (h *hashView) convert() {
	for h.gen == 0 {
		h.gen = rand.Uint64()
	}
	expireAt, _ := h.tx.ExpireAt(h.key)
	h.table = true
	h.tx.PutKind(h.key, typeHash|bitcask.Table, h.meta(), expireAt)
	h.blob.each(func(field, value []byte) {
		h.tx.PutMember(h.key, string(field), value)
		h.count++
	})
	h.blob = nil
}

func (h *hashView) store() {
	if !h.dirty {
		return
	}
	expireAt, _ := h.tx.ExpireAt(h.key)
	switch {
	case h.len() == 0:
		h.tx.Delete(h.key)
	case h.table:
		h.tx.PutKind(h.key, typeHash|bitcask.Table, h.meta(), expireAt)
	default:
		h.tx.PutKind(h.key, typeHash, h.blob, expireAt)
	}
}

func hset(tx *bitcask.Tx, args [][]byte, name string) (int, reply, error) {
	if len(args)%2 == 1 {
		return 0, errorReply("ERR wrong number of arguments for '" + name + "' command"), nil
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return 0, bad, err
	}
	added := 0
	for i := 2; i < len(args); i += 2 {
		isNew, err := h.set(args[i], args[i+1])
		if err != nil {
			return 0, nil, err
		}
		if isNew {
			added++
		}
	}
	h.store()
	return added, nil, nil
}

func cmdHSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	added, bad, err := hset(tx, args, "hset")
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(added), nil
}

func cmdHMSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	_, bad, err := hset(tx, args, "hmset")
	if bad != nil || err != nil {
		return bad, err
	}
	return okReply, nil
}

func cmdHSetNX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if _, found, err := h.get(args[2]); err != nil || found {
		return intReply(0), err
	}
	if _, err := h.set(args[2], args[3]); err != nil {
		return nil, err
	}
	h.store()
	return intReply(1), nil
}

func cmdHGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, found, err := h.get(args[2])
	if err != nil || !found {
		return nilReply, err
	}
	return bulkReply(value), nil
}

func cmdHMGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	out := make(arrayReply, len(args)-2)
	for i, field := range args[2:] {
		value, found, err := h.get(field)
		if err != nil {
			return nil, err
		}
		out[i] = nilReply
		if found {
			out[i] = bulkReply(value)
		}
	}
	return out, nil
}

func cmdHDel(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	deleted := 0
	for _, field := range args[2:] {
		if h.del(field) {
			deleted++
		}
	}
	h.store()
	return intReply(deleted), nil
}

func cmdHLen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(h.len()), nil
}

func cmdHExists(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	_, found, err := h.get(args[2])
	if err != nil || !found {
		return intReply(0), err
	}
	return intReply(1), nil
}

func cmdHStrlen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, _, err := h.get(args[2])
	return intReply(len(value)), err
}

func hashItems(tx *bitcask.Tx, key []byte, fields, values bool) (reply, error) {
	h, bad, err := openHash(tx, key)
	if bad != nil || err != nil {
		return bad, err
	}
	out := arrayReply{}
	err = h.each(values, func(field, value []byte) {
		if fields {
			out = append(out, bulkReply(field))
		}
		if values {
			out = append(out, bulkReply(value))
		}
	})
	return out, err
}

func cmdHGetAll(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return hashItems(tx, args[1], true, true)
}

func cmdHKeys(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return hashItems(tx, args[1], true, false)
}

func cmdHVals(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return hashItems(tx, args[1], false, true)
}

func cmdHIncrBy(tx *bitcask.Tx, args [][]byte) (reply, error) {
	incr, ok := parseInt(args[3])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, found, err := h.get(args[2])
	if err != nil {
		return nil, err
	}
	var current int64
	if found {
		if current, ok = parseInt(value); !ok {
			return errorReply("ERR hash value is not an integer"), nil
		}
	}
	if (incr > 0 && current > math.MaxInt64-incr) || (incr < 0 && current < math.MinInt64-incr) {
		return errorReply(errOverflow), nil
	}
	if _, err := h.set(args[2], strconv.AppendInt(nil, current+incr, 10)); err != nil {
		return nil, err
	}
	h.store()
	return intReply(current + incr), nil
}

func cmdHIncrByFloat(tx *bitcask.Tx, args [][]byte) (reply, error) {
	incr, ok := parseFloat(args[3])
	if !ok {
		return errorReply(errNotFloat), nil
	}
	if math.IsInf(incr, 0) {
		return errorReply("ERR value is NaN or Infinity"), nil
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	value, found, err := h.get(args[2])
	if err != nil {
		return nil, err
	}
	var current float64
	if found {
		if current, ok = parseFloat(value); !ok {
			return errorReply("ERR hash value is not a float"), nil
		}
	}
	result := current + incr
	if math.IsNaN(result) || math.IsInf(result, 0) {
		return errorReply(errFloatEdge), nil
	}
	if result == 0 {
		result = 0
	}
	out := strconv.AppendFloat(nil, result, 'f', -1, 64)
	if _, err := h.set(args[2], out); err != nil {
		return nil, err
	}
	h.store()
	return bulkReply(out), nil
}

func cmdHScan(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if _, err := strconv.ParseUint(string(args[2]), 10, 64); err != nil {
		return errorReply("ERR invalid cursor"), nil
	}
	match := func([]byte) bool { return true }
	values := true
	for i := 3; i < len(args); i++ {
		switch opt := upper(args[i]); {
		case opt == "NOVALUES":
			values = false
		case opt == "MATCH" && i+1 < len(args):
			pattern := string(args[i+1])
			match = func(field []byte) bool { return matchGlob(pattern, string(field)) }
			i++
		case opt == "COUNT" && i+1 < len(args):
			if n, ok := parseInt(args[i+1]); !ok {
				return errorReply(errNotInteger), nil
			} else if n < 1 {
				return errorReply(errSyntax), nil
			}
			i++
		default:
			return errorReply(errSyntax), nil
		}
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	items := arrayReply{}
	err = h.each(values, func(field, value []byte) {
		if !match(field) {
			return
		}
		items = append(items, bulkReply(field))
		if values {
			items = append(items, bulkReply(value))
		}
	})
	return arrayReply{bulkReply("0"), items}, err
}

func cmdHRandField(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if len(args) > 4 || (len(args) == 4 && upper(args[3]) != "WITHVALUES") {
		return errorReply(errSyntax), nil
	}
	var count int64
	if len(args) >= 3 {
		var ok bool
		if count, ok = parseInt(args[2]); !ok {
			return errorReply(errNotInteger), nil
		}
		if count < -maxRandomCount || count > maxRandomCount {
			return errorReply("ERR value is out of range"), nil
		}
	}
	h, bad, err := openHash(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	var fields, values [][]byte
	if err := h.each(len(args) == 4, func(field, value []byte) {
		fields, values = append(fields, field), append(values, value)
	}); err != nil {
		return nil, err
	}
	if len(args) == 2 {
		if len(fields) == 0 {
			return nilReply, nil
		}
		return bulkReply(fields[rand.IntN(len(fields))]), nil
	}
	var picks []int
	switch {
	case len(fields) == 0:
	case count < 0:
		for range -count {
			picks = append(picks, rand.IntN(len(fields)))
		}
	default:
		picks = rand.Perm(len(fields))[:min(int(count), len(fields))]
	}
	out := arrayReply{}
	for _, i := range picks {
		out = append(out, bulkReply(fields[i]))
		if len(args) == 4 {
			out = append(out, bulkReply(values[i]))
		}
	}
	return out, nil
}
