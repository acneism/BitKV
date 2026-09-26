package resp

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"math"
)

const (
	maxInline    = 64 << 10
	maxArgs      = 1 << 20
	smallBulk    = 64 << 10
	readerBuffer = 16 << 10
)

type ProtocolError struct {
	Msg string
}

func (e *ProtocolError) Error() string {
	return "Protocol error: " + e.Msg
}

type Reader struct {
	br      *bufio.Reader
	maxBulk int
}

func NewReader(r io.Reader, maxBulk int) *Reader {
	return &Reader{br: bufio.NewReaderSize(r, readerBuffer), maxBulk: maxBulk}
}

func (r *Reader) Buffered() int {
	return r.br.Buffered()
}

func (r *Reader) ReadCommand() ([][]byte, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != '*' {
		return inline(line), nil
	}
	n, ok := parseInt(line[1:])
	if !ok || n > maxArgs {
		return nil, &ProtocolError{"invalid multibulk length"}
	}
	if n <= 0 {
		return nil, nil
	}
	args := make([][]byte, 0, min(n, 1024))
	for range n {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if len(line) == 0 || line[0] != '$' {
			got := "EOL"
			if len(line) > 0 {
				got = fmt.Sprintf("'%c'", line[0])
			}
			return nil, &ProtocolError{"expected '$', got " + got}
		}
		size, ok := parseInt(line[1:])
		if !ok || size < 0 || size > r.maxBulk {
			return nil, &ProtocolError{"invalid bulk length"}
		}
		b, err := r.readBulk(size)
		if err != nil {
			return nil, err
		}
		args = append(args, b)
	}
	return args, nil
}

func (r *Reader) readLine() ([]byte, error) {
	line, err := r.br.ReadSlice('\n')
	if errors.Is(err, bufio.ErrBufferFull) {
		buf := append([]byte(nil), line...)
		for errors.Is(err, bufio.ErrBufferFull) {
			if len(buf) > maxInline {
				return nil, &ProtocolError{"too big inline request"}
			}
			line, err = r.br.ReadSlice('\n')
			buf = append(buf, line...)
		}
		line = buf
	}
	if err != nil {
		return nil, err
	}
	if len(line) > maxInline {
		return nil, &ProtocolError{"too big inline request"}
	}
	line = line[:len(line)-1]
	if n := len(line); n > 0 && line[n-1] == '\r' {
		line = line[:n-1]
	}
	return line, nil
}

func (r *Reader) readBulk(n int) ([]byte, error) {
	var b []byte
	if n <= smallBulk {
		b = make([]byte, n+2)
		if _, err := io.ReadFull(r.br, b); err != nil {
			return nil, err
		}
	} else {
		var buf bytes.Buffer
		if _, err := io.CopyN(&buf, r.br, int64(n)+2); err != nil {
			return nil, err
		}
		b = buf.Bytes()
	}
	if b[n] != '\r' || b[n+1] != '\n' {
		return nil, &ProtocolError{"invalid bulk terminator"}
	}
	return b[:n:n], nil
}

func inline(line []byte) [][]byte {
	fields := bytes.Fields(line)
	args := make([][]byte, len(fields))
	for i, f := range fields {
		args[i] = append([]byte(nil), f...)
	}
	return args
}

func parseInt(b []byte) (int, bool) {
	neg := false
	if len(b) > 0 && b[0] == '-' {
		neg = true
		b = b[1:]
	}
	if len(b) == 0 || len(b) > 18 {
		return 0, false
	}
	var n int64
	for _, c := range b {
		if c < '0' || c > '9' {
			return 0, false
		}
		n = n*10 + int64(c-'0')
	}
	if n > math.MaxInt {
		return 0, false
	}
	if neg {
		n = -n
	}
	return int(n), true
}
