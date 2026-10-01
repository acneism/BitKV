package server

import (
	"math"
	"strconv"

	"github.com/acneism/casketdb/internal/bitcask"
)

var stringCommands = map[string]command{
	"get":    {arity: 2, kind: kindRead, keys: oneKey, tx: cmdGet},
	"set":    {arity: -3, kind: kindWrite, keys: oneKey, tx: cmdSet},
	"setnx":  {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdSetNX},
	"setex":  {arity: 4, kind: kindWrite, keys: oneKey, tx: cmdSetEX},
	"psetex": {arity: 4, kind: kindWrite, keys: oneKey, tx: cmdPSetEX},
	"getdel": {arity: 2, kind: kindWrite, keys: oneKey, tx: cmdGetDel},
	"mget":   {arity: -2, kind: kindRead, keys: allArgs, tx: cmdMGet},
	"mset":   {arity: -3, kind: kindWrite, keys: pairs, tx: cmdMSet},
	"append": {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdAppend},
	"strlen": {arity: 2, kind: kindRead, keys: oneKey, tx: cmdStrlen},
	"incr":   {arity: 2, kind: kindWrite, keys: oneKey, tx: cmdIncr},
	"decr":   {arity: 2, kind: kindWrite, keys: oneKey, tx: cmdDecr},
	"incrby": {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdIncrBy},
	"decrby": {arity: 3, kind: kindWrite, keys: oneKey, tx: cmdDecrBy},
}

func cmdGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	value, found, err := tx.Get(string(args[1]))
	if err != nil || !found {
		return nilReply, err
	}
	return bulkReply(value), nil
}

func cmdSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key, value := string(args[1]), args[2]
	var nx, xx, keepTTL, get, hasExpire bool
	var expireAt int64
	for i := 3; i < len(args); i++ {
		switch opt := upper(args[i]); opt {
		case "NX":
			nx = true
		case "XX":
			xx = true
		case "GET":
			get = true
		case "KEEPTTL":
			keepTTL = true
		case "EX", "PX", "EXAT", "PXAT":
			if hasExpire || i+1 >= len(args) {
				return errorReply(errSyntax), nil
			}
			n, ok := parseInt(args[i+1])
			if !ok {
				return errorReply(errNotInteger), nil
			}
			at, ok := expireTime(tx.Now(), n, opt)
			if !ok || n <= 0 {
				return errorReply("ERR invalid expire time in 'set' command"), nil
			}
			expireAt, hasExpire = at, true
			i++
		default:
			return errorReply(errSyntax), nil
		}
	}
	if (nx && xx) || (keepTTL && hasExpire) {
		return errorReply(errSyntax), nil
	}
	var result reply = okReply
	existed := tx.Exists(key)
	if get {
		old, found, err := tx.Get(key)
		if err != nil {
			return nil, err
		}
		result, existed = nilReply, found
		if found {
			result = bulkReply(old)
		}
	}
	if (nx && existed) || (xx && !existed) {
		if !get {
			result = nilReply
		}
		return result, nil
	}
	if keepTTL {
		expireAt, _ = tx.ExpireAt(key)
	}
	tx.Put(key, value, expireAt)
	return result, nil
}

func cmdSetNX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	if tx.Exists(key) {
		return intReply(0), nil
	}
	tx.Put(key, args[2], 0)
	return intReply(1), nil
}

func cmdSetEX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return setWithExpire(tx, args, "EX", "setex")
}

func cmdPSetEX(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return setWithExpire(tx, args, "PX", "psetex")
}

func setWithExpire(tx *bitcask.Tx, args [][]byte, unit, name string) (reply, error) {
	n, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	at, ok := expireTime(tx.Now(), n, unit)
	if !ok || n <= 0 {
		return errorReply("ERR invalid expire time in '" + name + "' command"), nil
	}
	tx.Put(string(args[1]), args[3], at)
	return okReply, nil
}

func cmdGetDel(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	value, found, err := tx.Get(key)
	if err != nil || !found {
		return nilReply, err
	}
	tx.Delete(key)
	return bulkReply(value), nil
}

func cmdMGet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	out := make(arrayReply, len(args)-1)
	for i, key := range args[1:] {
		value, found, err := tx.Get(string(key))
		if err != nil {
			return nil, err
		}
		if found {
			out[i] = bulkReply(value)
		} else {
			out[i] = nilReply
		}
	}
	return out, nil
}

func cmdMSet(tx *bitcask.Tx, args [][]byte) (reply, error) {
	if len(args)%2 != 1 {
		return errorReply("ERR wrong number of arguments for 'mset' command"), nil
	}
	for i := 1; i < len(args); i += 2 {
		tx.Put(string(args[i]), args[i+1], 0)
	}
	return okReply, nil
}

func cmdAppend(tx *bitcask.Tx, args [][]byte) (reply, error) {
	key := string(args[1])
	old, _, err := tx.Get(key)
	if err != nil {
		return nil, err
	}
	expireAt, _ := tx.ExpireAt(key)
	value := make([]byte, 0, len(old)+len(args[2]))
	value = append(value, old...)
	value = append(value, args[2]...)
	tx.Put(key, value, expireAt)
	return intReply(len(value)), nil
}

func cmdStrlen(tx *bitcask.Tx, args [][]byte) (reply, error) {
	value, _, err := tx.Get(string(args[1]))
	if err != nil {
		return nil, err
	}
	return intReply(len(value)), nil
}

func cmdIncr(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return incrBy(tx, args[1], 1)
}

func cmdDecr(tx *bitcask.Tx, args [][]byte) (reply, error) {
	return incrBy(tx, args[1], -1)
}

func cmdIncrBy(tx *bitcask.Tx, args [][]byte) (reply, error) {
	n, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	return incrBy(tx, args[1], n)
}

func cmdDecrBy(tx *bitcask.Tx, args [][]byte) (reply, error) {
	n, ok := parseInt(args[2])
	if !ok {
		return errorReply(errNotInteger), nil
	}
	if n == math.MinInt64 {
		return errorReply("ERR decrement would overflow"), nil
	}
	return incrBy(tx, args[1], -n)
}

func incrBy(tx *bitcask.Tx, rawKey []byte, delta int64) (reply, error) {
	key := string(rawKey)
	value, found, err := tx.Get(key)
	if err != nil {
		return nil, err
	}
	var current int64
	if found {
		n, ok := parseInt(value)
		if !ok {
			return errorReply(errNotInteger), nil
		}
		current = n
	}
	if (delta > 0 && current > math.MaxInt64-delta) || (delta < 0 && current < math.MinInt64-delta) {
		return errorReply(errOverflow), nil
	}
	result := current + delta
	expireAt, _ := tx.ExpireAt(key)
	tx.Put(key, strconv.AppendInt(nil, result, 10), expireAt)
	return intReply(result), nil
}
