package server

import (
	"errors"

	"github.com/acneism/casketdb/internal/replica"
	"github.com/acneism/casketdb/internal/resp"
)

type reply interface {
	writeTo(w *resp.Writer)
}

type statusReply string

type errorReply string

type intReply int64

type bulkReply []byte

type nullReply struct{}

type nullArrayReply struct{}

type arrayReply []reply

type stringsReply []string

var (
	okReply  = statusReply("OK")
	nilReply = nullReply{}
)

func (r statusReply) writeTo(w *resp.Writer) { w.Simple(string(r)) }

func (r errorReply) writeTo(w *resp.Writer) { w.Error(string(r)) }

func (r intReply) writeTo(w *resp.Writer) { w.Integer(int64(r)) }

func (r bulkReply) writeTo(w *resp.Writer) { w.Bulk(r) }

func (nullReply) writeTo(w *resp.Writer) { w.Null() }

func (nullArrayReply) writeTo(w *resp.Writer) { w.NullArray() }

func (r arrayReply) writeTo(w *resp.Writer) {
	w.Array(len(r))
	for _, e := range r {
		e.writeTo(w)
	}
}

func (r stringsReply) writeTo(w *resp.Writer) {
	w.Array(len(r))
	for _, s := range r {
		w.BulkString(s)
	}
}

func boolReply(b bool) intReply {
	if b {
		return 1
	}
	return 0
}

func storageError(err error) errorReply {
	switch {
	case errors.Is(err, replica.ErrNotLeader):
		return errorReply("READONLY You can't write against a read only replica.")
	case errors.Is(err, replica.ErrUnconfirmed):
		return errorReply("TRYAGAIN No leader confirmed the read, retry.")
	}
	return errorReply("ERR " + err.Error())
}
