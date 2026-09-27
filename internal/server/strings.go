package server

import (
	"math"
	"strconv"

	"github.com/acneism/casketdb/internal/bitcask"
)

func absoluteExpire(now int64, unit string, n int64) (int64, bool) {
	if n <= 0 {
		return 0, false
	}
	switch unit {
	case "EX":
		if n > (math.MaxInt64-now)/1000 {
			return 0, false
		}
		return now + n*1000, true
	case "PX":
		if n > math.MaxInt64-now {
			return 0, false
		}
		return now + n, true
	case "EXAT":
		if n > math.MaxInt64/1000 {
			return 0, false
		}
		return n * 1000, true
	default:
		return n, true
	}
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
			at, ok := absoluteExpire(tx.Now(), opt, n)
			if !ok {
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
	var old []byte
	var existed bool
	if get {
		var err error
		if old, existed, err = tx.Get(key); err != nil {
			return nil, err
		}
	} else {
		existed = tx.Exists(key)
	}
	var result reply = okReply
	switch {
	case get && existed:
		result = bulkReply(old)
	case get:
		result = nilReply
	}
	if (nx && existed) || (xx && !existed) {
		if get {
			return result, nil
		}
		return nilReply, nil
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
	at, ok := absoluteExpire(tx.Now(), unit, n)
	if !ok {
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
