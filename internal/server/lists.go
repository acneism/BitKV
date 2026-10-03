package server

import (
	"bytes"
	"encoding/binary"
	"math"
	"slices"

	"github.com/acneism/casketdb/internal/bitcask"
)

var listCommands = map[string]command{
	"lpush":     {arity: -3, kind: kindWrite, keys: oneKey, acl: catList | catFast, tx: cmdLPush},
	"rpush":     {arity: -3, kind: kindWrite, keys: oneKey, acl: catList | catFast, tx: cmdRPush},
	"lpushx":    {arity: -3, kind: kindWrite, keys: oneKey, acl: catList | catFast, tx: cmdLPushX},
	"rpushx":    {arity: -3, kind: kindWrite, keys: oneKey, acl: catList | catFast, tx: cmdRPushX},
	"lpop":      {arity: -2, kind: kindWrite, keys: oneKey, acl: catList | catFast, tx: cmdLPop},
	"rpop":      {arity: -2, kind: kindWrite, keys: oneKey, acl: catList | catFast, tx: cmdRPop},
	"llen":      {arity: 2, kind: kindRead, keys: oneKey, acl: catList | catFast, tx: cmdLLen},
	"lindex":    {arity: 3, kind: kindRead, keys: oneKey, acl: catList, tx: cmdLIndex},
	"lrange":    {arity: 4, kind: kindRead, keys: oneKey, acl: catList, tx: cmdLRange},
	"lset":      {arity: 4, kind: kindWrite, keys: oneKey, acl: catList, tx: cmdLSet},
	"lrem":      {arity: 4, kind: kindWrite, keys: oneKey, acl: catList, tx: cmdLRem},
	"ltrim":     {arity: 4, kind: kindWrite, keys: oneKey, acl: catList, tx: cmdLTrim},
	"linsert":   {arity: 5, kind: kindWrite, keys: oneKey, acl: catList, tx: cmdLInsert},
	"lpos":      {arity: -3, kind: kindRead, keys: oneKey, acl: catList, tx: cmdLPos},
	"lmove":     {arity: 5, kind: kindWrite, keys: keySpec{first: 1, last: 2, step: 1}, acl: catList, tx: cmdLMove},
	"rpoplpush": {arity: 3, kind: kindWrite, keys: keySpec{first: 1, last: 2, step: 1}, acl: catList, tx: cmdRPopLPush},
	"lmpop":     {arity: -4, kind: kindWrite, keys: keySpec{numkeys: 1}, acl: catList, tx: cmdLMPop},
}

type listView struct {
	tx    *bitcask.Tx
	key   string
	table bool
	gen   uint64
	head  int64
	tail  int64
	items [][]byte
	dirty bool
}

func openList(tx *bitcask.Tx, key []byte) (*listView, reply, error) {
	value, kind, found, err := tx.GetKind(string(key))
	if err != nil {
		return nil, nil, err
	}
	l := &listView{tx: tx, key: string(key)}
	switch {
	case !found:
	case kind == typeList:
		for rest := value; len(rest) > 0; {
			n, k := binary.Uvarint(rest)
			if k <= 0 || n > uint64(len(rest)-k) {
				break
			}
			l.items = append(l.items, rest[k:k+int(n)])
			rest = rest[k+int(n):]
		}
	case kind == typeList|bitcask.Table && len(value) >= 8:
		l.table, l.gen = true, binary.LittleEndian.Uint64(value)
		var k int
		l.head, k = binary.Varint(value[8:])
		l.tail, _ = binary.Varint(value[8+max(k, 0):])
	default:
		return nil, errorReply(errWrongType), nil
	}
	return l, nil, nil
}

func seqKey(i int64) string {
	return string(binary.BigEndian.AppendUint64(nil, uint64(i)))
}

func (l *listView) len() int64 {
	if l.table {
		return l.tail - l.head
	}
	return int64(len(l.items))
}

func (l *listView) at(i int64) ([]byte, error) {
	if !l.table {
		return l.items[i], nil
	}
	v, _, err := l.tx.GetMember(l.key, seqKey(l.head+i))
	return v, err
}

func (l *listView) slice(from, to int64) ([][]byte, error) {
	if !l.table {
		return l.items[from : to+1], nil
	}
	out := make([][]byte, 0, to-from+1)
	for i := from; i <= to; i++ {
		v, err := l.at(i)
		if err != nil {
			return nil, err
		}
		out = append(out, v)
	}
	return out, nil
}

func (l *listView) all() ([][]byte, error) {
	if l.len() == 0 {
		return nil, nil
	}
	return l.slice(0, l.len()-1)
}

func (l *listView) push(left bool, v []byte) {
	l.dirty = true
	if !l.table {
		if left {
			l.items = append([][]byte{v}, l.items...)
		} else {
			l.items = append(l.items, v)
		}
		if len(v) > maxListpackItem || len(l.items) > maxListpackLen {
			l.toTable(l.items)
		}
		return
	}
	if left {
		l.head--
		l.tx.PutMember(l.key, seqKey(l.head), v)
	} else {
		l.tx.PutMember(l.key, seqKey(l.tail), v)
		l.tail++
	}
}

func (l *listView) pop(left bool) ([]byte, error) {
	l.dirty = true
	if !l.table {
		var v []byte
		if left {
			v, l.items = l.items[0], l.items[1:]
		} else {
			v, l.items = l.items[len(l.items)-1], l.items[:len(l.items)-1]
		}
		return v, nil
	}
	seq := l.tail - 1
	if left {
		seq = l.head
	}
	v, _, err := l.tx.GetMember(l.key, seqKey(seq))
	if err != nil {
		return nil, err
	}
	l.tx.DeleteMember(l.key, seqKey(seq))
	if left {
		l.head++
	} else {
		l.tail--
	}
	return v, nil
}

func (l *listView) drop(left bool) {
	l.dirty = true
	if left {
		l.tx.DeleteMember(l.key, seqKey(l.head))
		l.head++
	} else {
		l.tail--
		l.tx.DeleteMember(l.key, seqKey(l.tail))
	}
}

func (l *listView) set(i int64, v []byte) {
	l.dirty = true
	if l.table {
		l.tx.PutMember(l.key, seqKey(l.head+i), v)
		return
	}
	l.items = append([][]byte(nil), l.items...)
	l.items[i] = v
	if len(v) > maxListpackItem {
		l.toTable(l.items)
	}
}

func (l *listView) replace(items [][]byte) {
	l.dirty = true
	if !l.table && len(items) <= maxListpackLen && !slices.ContainsFunc(items, func(v []byte) bool { return len(v) > maxListpackItem }) {
		l.items = items
		return
	}
	l.toTable(items)
}

func (l *listView) toTable(items [][]byte) {
	l.table, l.gen, l.head, l.tail, l.items = true, newGen(), 0, int64(len(items)), nil
	if len(items) == 0 {
		return
	}
	expireAt, _ := l.tx.ExpireAt(l.key)
	l.tx.PutKind(l.key, typeList|bitcask.Table, l.meta(), expireAt)
	for i, v := range items {
		l.tx.PutMember(l.key, seqKey(int64(i)), v)
	}
}

func (l *listView) meta() []byte {
	b := binary.LittleEndian.AppendUint64(nil, l.gen)
	return binary.AppendVarint(binary.AppendVarint(b, l.head), l.tail)
}

func (l *listView) store() {
	if !l.dirty {
		return
	}
	expireAt, _ := l.tx.ExpireAt(l.key)
	switch {
	case l.len() == 0:
		l.tx.Delete(l.key)
	case l.table:
		l.tx.PutKind(l.key, typeList|bitcask.Table, l.meta(), expireAt)
	default:
		var b []byte
		for _, v := range l.items {
			b = append(binary.AppendUvarint(b, uint64(len(v))), v...)
		}
		l.tx.PutKind(l.key, typeList, b, expireAt)
	}
}

func listRange(n, start, end int64) (int64, int64, bool) {
	if start < 0 {
		start += n
	}
	if end < 0 {
		end += n
	}
	start = max(start, 0)
	if start > end || start >= n {
		return 0, 0, false
	}
	return start, min(end, n-1), true
}

func push(tx *bitcask.Tx, args [][]byte, left, mustExist bool) (reply, error) {
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if mustExist && l.len() == 0 {
		return intReply(0), nil
	}
	for _, v := range args[2:] {
		l.push(left, v)
	}
	l.store()
	return intReply(l.len()), nil
}

func cmdLPush(tx *bitcask.Tx, args [][]byte) (reply, error)  { return push(tx, args, true, false) }
func cmdRPush(tx *bitcask.Tx, args [][]byte) (reply, error)  { return push(tx, args, false, false) }
func cmdLPushX(tx *bitcask.Tx, args [][]byte) (reply, error) { return push(tx, args, true, true) }
func cmdRPushX(tx *bitcask.Tx, args [][]byte) (reply, error) { return push(tx, args, false, true) }

func pop(tx *bitcask.Tx, args [][]byte, left bool) (reply, error) {
	if len(args) > 3 {
		return errorReply(errSyntax), nil
	}
	count := int64(1)
	if len(args) == 3 {
		var ok bool
		if count, ok = parseInt(args[2]); !ok || count < 0 {
			return errorReply("ERR value is out of range, must be positive"), nil
		}
	}
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if l.len() == 0 {
		if len(args) == 3 {
			return nullArrayReply{}, nil
		}
		return nilReply, nil
	}
	popped, err := l.popN(left, count)
	if err != nil {
		return nil, err
	}
	if len(args) == 3 {
		return membersReply(popped), nil
	}
	return bulkReply(popped[0]), nil
}

func (l *listView) popN(left bool, count int64) ([][]byte, error) {
	var popped [][]byte
	for range min(count, l.len()) {
		v, err := l.pop(left)
		if err != nil {
			return nil, err
		}
		popped = append(popped, v)
	}
	l.store()
	return popped, nil
}

func cmdLPop(tx *bitcask.Tx, args [][]byte) (reply, error) { return pop(tx, args, true) }
func cmdRPop(tx *bitcask.Tx, args [][]byte) (reply, error) { return pop(tx, args, false) }

func cmdLMPop(tx *bitcask.Tx, args [][]byte) (reply, error) {
	p, bad := parseMPop(args, 1, "LEFT", "RIGHT")
	if bad != nil {
		return bad, nil
	}
	return lmpop(tx, p)
}

func lmpop(tx *bitcask.Tx, p mpop) (reply, error) {
	for _, key := range p.keys {
		l, bad, err := openList(tx, key)
		if bad != nil || err != nil {
			return bad, err
		}
		if l.len() == 0 {
			continue
		}
		popped, err := l.popN(p.first, p.count)
		return arrayReply{bulkReply(key), membersReply(popped)}, err
	}
	return nullArrayReply{}, nil
}

func cmdLLen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	return intReply(l.len()), nil
}

func listIndex(l *listView, arg []byte) (int64, bool, reply) {
	i, ok := parseInt(arg)
	if !ok {
		return 0, false, errorReply(errNotInteger)
	}
	if i < 0 {
		i += l.len()
	}
	return i, i >= 0 && i < l.len(), nil
}

func cmdLIndex(tx *bitcask.Tx, args [][]byte) (reply, error) {
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	i, ok, bad := listIndex(l, args[2])
	if bad != nil || !ok {
		return cmpReply(bad, nilReply), nil
	}
	v, err := l.at(i)
	if err != nil {
		return nil, err
	}
	return bulkReply(v), nil
}

func cmpReply(bad, fallback reply) reply {
	if bad != nil {
		return bad
	}
	return fallback
}

func cmdLRange(tx *bitcask.Tx, args [][]byte) (reply, error) {
	start, ok1 := parseInt(args[2])
	end, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		return errorReply(errNotInteger), nil
	}
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	from, to, ok := listRange(l.len(), start, end)
	if !ok {
		return arrayReply{}, nil
	}
	items, err := l.slice(from, to)
	return membersReply(items), err
}

func cmdLSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if l.len() == 0 {
		return errorReply("ERR no such key"), nil
	}
	i, ok, bad := listIndex(l, args[2])
	if bad != nil || !ok {
		return cmpReply(bad, errorReply("ERR index out of range")), nil
	}
	l.set(i, args[3])
	l.store()
	return okReply, nil
}

func cmdLRem(tx *bitcask.Tx, args [][]byte) (reply, error) {
	count, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	items, err := l.all()
	if err != nil {
		return nil, err
	}
	keep := make([]bool, len(items))
	removed := int64(0)
	for j := range items {
		i := j
		if count < 0 {
			i = len(items) - 1 - j
		}
		keep[i] = !bytes.Equal(items[i], args[3]) || (count != 0 && removed == abs(count))
		if !keep[i] {
			removed++
		}
	}
	if removed > 0 {
		var kept [][]byte
		for i, v := range items {
			if keep[i] {
				kept = append(kept, v)
			}
		}
		l.replace(kept)
		l.store()
	}
	return intReply(removed), nil
}

func abs(n int64) int64 {
	if n < 0 {
		if n == math.MinInt64 {
			return math.MaxInt64
		}
		return -n
	}
	return n
}

func cmdLTrim(tx *bitcask.Tx, args [][]byte) (reply, error) {
	start, ok1 := parseInt(args[2])
	end, ok2 := parseInt(args[3])
	if !ok1 || !ok2 {
		return errorReply(errNotInteger), nil
	}
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	n := l.len()
	from, to, ok := listRange(n, start, end)
	switch {
	case n == 0:
		return okReply, nil
	case !ok:
		l.replace(nil)
	case l.table && n-(to-from+1) <= to-from+1:
		for range from {
			l.drop(true)
		}
		for range n - 1 - to {
			l.drop(false)
		}
	default:
		kept, err := l.slice(from, to)
		if err != nil {
			return nil, err
		}
		l.replace(append([][]byte(nil), kept...))
	}
	l.store()
	return okReply, nil
}

func cmdLInsert(tx *bitcask.Tx, args [][]byte) (reply, error) {
	where := upper(args[2])
	if where != "BEFORE" && where != "AFTER" {
		return errorReply(errSyntax), nil
	}
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if l.len() == 0 {
		return intReply(0), nil
	}
	items, err := l.all()
	if err != nil {
		return nil, err
	}
	i := slices.IndexFunc(items, func(v []byte) bool { return bytes.Equal(v, args[3]) })
	if i < 0 {
		return intReply(-1), nil
	}
	if where == "AFTER" {
		i++
	}
	l.replace(slices.Insert(append([][]byte(nil), items...), i, args[4]))
	l.store()
	return intReply(l.len()), nil
}

func cmdLPos(tx *bitcask.Tx, args [][]byte) (reply, error) {
	rank, count, maxLen := int64(1), int64(-1), int64(0)
	for i := 3; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return errorReply(errSyntax), nil
		}
		n, ok := parseInt(args[i+1])
		if !ok {
			return errorReply(errNotInteger), nil
		}
		switch upper(args[i]) {
		case "RANK":
			if n == 0 {
				return errorReply("ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list"), nil
			}
			if n == math.MinInt64 {
				return errorReply("ERR value is out of range"), nil
			}
			rank = n
		case "COUNT":
			if n < 0 {
				return errorReply("ERR COUNT can't be negative"), nil
			}
			count = n
		case "MAXLEN":
			if n < 0 {
				return errorReply("ERR MAXLEN can't be negative"), nil
			}
			maxLen = n
		default:
			return errorReply(errSyntax), nil
		}
	}
	l, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	var found arrayReply
	skip := abs(rank) - 1
	for j := int64(0); j < l.len() && (maxLen == 0 || j < maxLen); j++ {
		i := j
		if rank < 0 {
			i = l.len() - 1 - j
		}
		v, err := l.at(i)
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(v, args[2]) {
			continue
		}
		if skip > 0 {
			skip--
			continue
		}
		found = append(found, intReply(i))
		if count < 0 || (count > 0 && int64(len(found)) == count) {
			break
		}
	}
	switch {
	case count >= 0:
		return append(arrayReply{}, found...), nil
	case len(found) == 0:
		return nilReply, nil
	}
	return found[0], nil
}

func lmove(tx *bitcask.Tx, args [][]byte, fromLeft, toLeft bool) (reply, error) {
	src, bad, err := openList(tx, args[1])
	if bad != nil || err != nil {
		return bad, err
	}
	if src.len() == 0 {
		return nilReply, nil
	}
	dst := src
	if string(args[1]) != string(args[2]) {
		if dst, bad, err = openList(tx, args[2]); bad != nil || err != nil {
			return bad, err
		}
	}
	v, err := src.pop(fromLeft)
	if err != nil {
		return nil, err
	}
	dst.push(toLeft, v)
	src.store()
	if dst != src {
		dst.store()
	}
	return bulkReply(v), nil
}

func side(arg []byte) (left, ok bool) {
	switch upper(arg) {
	case "LEFT":
		return true, true
	case "RIGHT":
		return false, true
	}
	return false, false
}

func cmdLMove(tx *bitcask.Tx, args [][]byte) (reply, error) {
	from, ok1 := side(args[3])
	to, ok2 := side(args[4])
	if !ok1 || !ok2 {
		return errorReply(errSyntax), nil
	}
	return lmove(tx, args, from, to)
}

func cmdRPopLPush(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return lmove(tx, args, false, true)
}
