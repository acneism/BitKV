package server

import (
	"math"
	"strconv"
	"strings"

	"github.com/acneism/casketdb/internal/bitcask"
)

var keyCommands = map[string]command{
	"del":       {arity: -2, kind: kindWrite, keys: allArgs, acl: catKeyspace, tx: cmdDel},
	"unlink":    {arity: -2, kind: kindWrite, keys: allArgs, acl: catKeyspace | catFast, tx: cmdDel},
	"exists":    {arity: -2, kind: kindRead, keys: allArgs, acl: catKeyspace | catFast, tx: cmdExists},
	"type":      {arity: 2, kind: kindRead, keys: oneKey, acl: catKeyspace | catFast, tx: cmdType},
	"object":    {arity: -2, kind: kindRead, keys: keySpec{first: 2, last: 2, step: 1}, acl: catKeyspace, tx: cmdObject},
	"keys":      {arity: 2, kind: kindRead, global: true, acl: catKeyspace | catDangerous, tx: cmdKeys},
	"scan":      {arity: -2, kind: kindRead, global: true, acl: catKeyspace, tx: cmdScan},
	"dbsize":    {arity: 1, kind: kindRead, global: true, acl: catKeyspace | catFast, tx: cmdDBSize},
	"expire":    {arity: -3, kind: kindWrite, keys: oneKey, acl: catKeyspace | catFast, tx: cmdExpire},
	"pexpire":   {arity: -3, kind: kindWrite, keys: oneKey, acl: catKeyspace | catFast, tx: cmdPExpire},
	"expireat":  {arity: -3, kind: kindWrite, keys: oneKey, acl: catKeyspace | catFast, tx: cmdExpireAt},
	"pexpireat": {arity: -3, kind: kindWrite, keys: oneKey, acl: catKeyspace | catFast, tx: cmdPExpireAt},
	"ttl":       {arity: 2, kind: kindRead, keys: oneKey, acl: catKeyspace | catFast, tx: cmdTTL},
	"pttl":      {arity: 2, kind: kindRead, keys: oneKey, acl: catKeyspace | catFast, tx: cmdPTTL},
	"persist":   {arity: 2, kind: kindWrite, keys: oneKey, acl: catKeyspace | catFast, tx: cmdPersist},
}

func cmdDel(tx *bitcask.Tx, args [][]byte) (reply, error) {
	var deleted intReply
	for _, key := range args[1:] {
		if tx.Delete(string(key)) {
			deleted++
		}
	}
	return deleted, nil
}

func cmdExists(tx *bitcask.Tx, args [][]byte) (reply, error) {
	var count intReply
	for _, key := range args[1:] {
		if tx.Exists(string(key)) {
			count++
		}
	}
	return count, nil
}

const (
	typeString bitcask.Kind = 0
	typeList   bitcask.Kind = 1
	typeSet    bitcask.Kind = 2
	typeZSet   bitcask.Kind = 3
	typeHash   bitcask.Kind = 4
	typeStream bitcask.Kind = 6
)

var typeNames = map[bitcask.Kind]string{typeString: "string", typeList: "list", typeSet: "set", typeZSet: "zset", typeHash: "hash", typeStream: "stream"}

func cmdType(tx *bitcask.Tx, args [][]byte) (reply, error) {
	_, kind, found, err := tx.GetKind(string(args[1]))
	if err != nil || !found {
		return statusReply("none"), err
	}
	return statusReply(typeNames[kind&^bitcask.Table]), nil
}

func cmdObject(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if upper(args[1]) != "ENCODING" || len(args) != 3 {
		return unknownSubcommand(args), nil
	}
	value, kind, found, err := tx.GetKind(string(args[2]))
	if err != nil || !found {
		return nilReply, err
	}
	if kind&bitcask.Table != 0 {
		return bulkReply("hashtable"), nil
	}
	if kind != typeString {
		return bulkReply("listpack"), nil
	}
	if _, ok := parseInt(value); ok {
		return bulkReply("int"), nil
	}
	if len(value) <= 44 {
		return bulkReply("embstr"), nil
	}
	return bulkReply("raw"), nil
}

func cmdDBSize(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return intReply(tx.Len()), nil
}

func globMatcher(pattern string) func(string) bool {
	if pattern == "*" {
		return nil
	}
	return func(key string) bool { return matchGlob(pattern, key) }
}

func cmdKeys(tx *bitcask.Tx, args [][]byte) (reply, error) {
	_, keys := tx.Scan(0, math.MaxInt, globMatcher(string(args[1])), nil)
	return stringsReply(keys), nil
}

func cmdScan(tx *bitcask.Tx, args [][]byte) (reply, error) {
	cursor, err := strconv.ParseUint(string(args[1]), 10, 64)
	if err != nil {
		return errorReply("ERR invalid cursor"), nil
	}
	count := 10
	pattern := "*"
	var ofKind func(bitcask.Kind) bool
	for i := 2; i < len(args); i += 2 {
		if i+1 >= len(args) {
			return errorReply(errSyntax), nil
		}
		switch upper(args[i]) {
		case "MATCH":
			pattern = string(args[i+1])
		case "COUNT":
			n, ok := parseInt(args[i+1])
			if !ok {
				return errorReply(errNotInteger), nil
			}
			if n < 1 {
				return errorReply(errSyntax), nil
			}
			count = int(min(n, math.MaxInt32))
		case "TYPE":
			name := strings.ToLower(string(args[i+1]))
			ofKind = func(k bitcask.Kind) bool { return typeNames[k&^bitcask.Table] == name }
		default:
			return errorReply(errSyntax), nil
		}
	}
	next, keys := tx.Scan(cursor, count, globMatcher(pattern), ofKind)
	return arrayReply{bulkReply(strconv.FormatUint(next, 10)), stringsReply(keys)}, nil
}

func cmdExpire(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return expireGeneric(tx, args, "EX")
}

func cmdPExpire(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return expireGeneric(tx, args, "PX")
}

func cmdExpireAt(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return expireGeneric(tx, args, "EXAT")
}

func cmdPExpireAt(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return expireGeneric(tx, args, "PXAT")
}

func expireTime(now, n int64, unit string) (int64, bool) {
	if unit == "EX" || unit == "EXAT" {
		if n > math.MaxInt64/1000 || n < math.MinInt64/1000 {
			return 0, false
		}
		n *= 1000
	}
	if unit == "EXAT" || unit == "PXAT" {
		return n, true
	}
	if (n > 0 && now > math.MaxInt64-n) || (n < 0 && now < math.MinInt64-n) {
		return 0, false
	}
	return now + n, true
}

func expireGeneric(tx *bitcask.Tx, args [][]byte, unit string) (reply, error) {
	n, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	var nx, xx, gt, lt bool
	for _, a := range args[3:] {
		switch upper(a) {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GT":
			gt = true
		case "LT":
			lt = true
		default:
			return errorReply("ERR Unsupported option " + truncate(a, 128)), nil
		}
	}
	if nx && (xx || gt || lt) {
		return errorReply("ERR NX and XX, GT or LT options at the same time are not compatible"), nil
	}
	if gt && lt {
		return errorReply("ERR GT and LT options at the same time are not compatible"), nil
	}
	when, ok := expireTime(tx.Now(), n, unit)
	if !ok {
		return errorReply("ERR invalid expire time in '" + strings.ToLower(string(args[0])) + "' command"), nil
	}
	key := string(args[1])
	current, exists := tx.ExpireAt(key)
	if !exists {
		return intReply(0), nil
	}
	switch {
	case nx && current != 0,
		xx && current == 0,
		gt && (current == 0 || when <= current),
		lt && current != 0 && when >= current:
		return intReply(0), nil
	}
	if when <= tx.Now() {
		tx.Delete(key)
		return intReply(1), nil
	}
	return intReply(1), rewrite(tx, key, when)
}

func rewrite(tx *bitcask.Tx, key string, expireAt int64) error {
	value, kind, _, err := tx.GetKind(key)
	if err == nil {
		tx.PutKind(key, kind, value, expireAt)
	}
	return err
}

func cmdTTL(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return ttlGeneric(tx, args[1], false), nil
}

func cmdPTTL(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return ttlGeneric(tx, args[1], true), nil
}

func ttlGeneric(tx *bitcask.Tx, key []byte, millis bool) reply {
	expireAt, exists := tx.ExpireAt(string(key))
	switch {
	case !exists:
		return intReply(-2)
	case expireAt == 0:
		return intReply(-1)
	}
	left := max(expireAt-tx.Now(), 0)
	if !millis {
		left = (left + 500) / 1000
	}
	return intReply(left)
}

func cmdPersist(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	expireAt, exists := tx.ExpireAt(key)
	if !exists || expireAt == 0 {
		return intReply(0), nil
	}
	return intReply(1), rewrite(tx, key, 0)
}
