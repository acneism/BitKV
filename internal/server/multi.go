package server

import "github.com/acneism/BitKV/internal/bitcask"

type queued struct {
	cmd  command
	args [][]byte
}

var okCommand = command{arity: 1, kind: kindPure, tx: func(*bitcask.Tx, [][]byte) (reply, error) {
	return okReply, nil
}}

func (c *client) resetMulti() {
	c.multi, c.dirty, c.queue, c.watched = false, false, nil, nil
}

func cmdMulti(s *Server, c *client, args [][]byte) reply {
	if c.multi {
		return errorReply("ERR MULTI calls can not be nested")
	}
	c.multi = true
	return okReply
}

func cmdDiscard(s *Server, c *client, args [][]byte) reply {
	if !c.multi {
		return errorReply("ERR DISCARD without MULTI")
	}
	c.resetMulti()
	return okReply
}

func cmdWatch(s *Server, c *client, args [][]byte) reply {
	if c.multi {
		return errorReply("ERR WATCH inside MULTI is not allowed")
	}
	keys := allArgs.extract(args, nil)
	err := s.db.View(bitcask.Keys(keys...), func(tx *bitcask.Tx) error {
		if c.watched == nil {
			c.watched = make(map[string]bitcask.Version)
		}
		for _, key := range keys {
			if _, ok := c.watched[key]; !ok {
				c.watched[key] = tx.Version(key)
			}
		}
		return nil
	})
	if err != nil {
		return storageError(err)
	}
	return okReply
}

func cmdUnwatch(s *Server, c *client, args [][]byte) reply {
	if c.multi {
		c.queue = append(c.queue, queued{cmd: okCommand, args: args})
		return statusReply("QUEUED")
	}
	c.watched = nil
	return okReply
}

func cmdExec(s *Server, c *client, args [][]byte) reply {
	if !c.multi {
		return errorReply("ERR EXEC without MULTI")
	}
	queue, watched, dirty := c.queue, c.watched, c.dirty
	c.resetMulti()
	if dirty {
		return errorReply("EXECABORT Transaction discarded because of previous errors.")
	}
	var keys []string
	writes, global := false, false
	for _, q := range queue {
		keys = q.cmd.keys.extract(q.args, keys)
		writes = writes || q.cmd.kind == kindWrite
		global = global || q.cmd.global
	}
	for key := range watched {
		keys = append(keys, key)
	}
	scope := bitcask.Keys(keys...)
	if global {
		scope = bitcask.All()
	}
	var results arrayReply
	aborted := false
	run := func(tx *bitcask.Tx) error {
		for key, v := range watched {
			if tx.Version(key) != v {
				aborted = true
				return nil
			}
		}
		results = make(arrayReply, 0, len(queue))
		for _, q := range queue {
			r, err := q.cmd.tx(tx, q.args)
			if err != nil {
				return err
			}
			results = append(results, r)
		}
		return nil
	}
	var err error
	if writes {
		err = s.update(scope, run)
	} else {
		err = s.db.View(scope, run)
	}
	switch {
	case err != nil:
		return storageError(err)
	case aborted:
		return nullArrayReply{}
	}
	s.processed.Add(int64(len(queue)))
	return results
}
