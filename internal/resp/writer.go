package resp

import (
	"bufio"
	"io"
	"strconv"
	"strings"
)

type Writer struct {
	bw  *bufio.Writer
	num []byte
}

func NewWriter(w io.Writer) *Writer {
	return &Writer{bw: bufio.NewWriterSize(w, 16<<10)}
}

func (w *Writer) Simple(s string) {
	w.line('+', s)
}

func (w *Writer) Error(s string) {
	w.line('-', s)
}

func (w *Writer) line(p byte, s string) {
	w.bw.WriteByte(p)
	w.bw.WriteString(oneLine(s))
	w.bw.WriteString("\r\n")
}

func (w *Writer) Integer(n int64) {
	w.prefixed(':', n)
}

func (w *Writer) Bulk(b []byte) {
	w.prefixed('$', int64(len(b)))
	w.bw.Write(b)
	w.bw.WriteString("\r\n")
}

func (w *Writer) BulkString(s string) {
	w.prefixed('$', int64(len(s)))
	w.bw.WriteString(s)
	w.bw.WriteString("\r\n")
}

func (w *Writer) Null() {
	w.bw.WriteString("$-1\r\n")
}

func (w *Writer) Array(n int) {
	w.prefixed('*', int64(n))
}

func (w *Writer) NullArray() {
	w.bw.WriteString("*-1\r\n")
}

func (w *Writer) Flush() error {
	return w.bw.Flush()
}

func (w *Writer) prefixed(p byte, n int64) {
	w.num = append(w.num[:0], p)
	w.num = strconv.AppendInt(w.num, n, 10)
	w.num = append(w.num, '\r', '\n')
	w.bw.Write(w.num)
}

func oneLine(s string) string {
	if !strings.ContainsAny(s, "\r\n") {
		return s
	}
	return strings.NewReplacer("\r", " ", "\n", " ").Replace(s)
}
