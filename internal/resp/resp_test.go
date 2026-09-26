package resp

import (
	"bytes"
	"errors"
	"io"
	"strings"
	"testing"
)

func toStrings(args [][]byte) []string {
	out := make([]string, len(args))
	for i, a := range args {
		out[i] = string(a)
	}
	return out
}

func equal(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}

func TestReadCommand(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n", []string{"GET", "k"}},
		{"*1\r\n$0\r\n\r\n", []string{""}},
		{"*3\r\n$3\r\nSET\r\n$1\r\nk\r\n$4\r\na\r\nb\r\n", []string{"SET", "k", "a\r\nb"}},
		{"PING\r\n", []string{"PING"}},
		{"set  a   b\n", []string{"set", "a", "b"}},
	}
	for _, tt := range tests {
		args, err := NewReader(strings.NewReader(tt.in), 1<<20).ReadCommand()
		if err != nil {
			t.Fatalf("%q: %v", tt.in, err)
		}
		if got := toStrings(args); !equal(got, tt.want) {
			t.Fatalf("%q: got %q, want %q", tt.in, got, tt.want)
		}
	}
}

func TestPipelinedCommands(t *testing.T) {
	r := NewReader(strings.NewReader("*1\r\n$4\r\nPING\r\n*2\r\n$4\r\nECHO\r\n$2\r\nhi\r\nPING\r\n"), 1<<20)
	for _, want := range [][]string{{"PING"}, {"ECHO", "hi"}, {"PING"}} {
		args, err := r.ReadCommand()
		if err != nil {
			t.Fatal(err)
		}
		if got := toStrings(args); !equal(got, want) {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	if _, err := r.ReadCommand(); !errors.Is(err, io.EOF) {
		t.Fatalf("err = %v, want EOF", err)
	}
}

func TestProtocolErrors(t *testing.T) {
	inputs := []string{
		"*1\r\n:5\r\n",
		"*1\r\n$3\r\nabcde\r\n",
		"*x\r\n",
		"*1\r\n$-5\r\n",
		"*1\r\n$2000000\r\n",
		"*9999999999\r\n",
		strings.Repeat("x", 70000) + "\r\n",
	}
	for _, in := range inputs {
		_, err := NewReader(strings.NewReader(in), 1<<20).ReadCommand()
		var pe *ProtocolError
		if !errors.As(err, &pe) {
			t.Errorf("%.20q: err = %v, want protocol error", in, err)
		}
	}
}

func TestLargeBulk(t *testing.T) {
	value := strings.Repeat("v", 200000)
	in := "*2\r\n$4\r\nECHO\r\n$200000\r\n" + value + "\r\n"
	args, err := NewReader(strings.NewReader(in), 1<<20).ReadCommand()
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 2 || string(args[1]) != value {
		t.Fatal("large bulk string was not read correctly")
	}
}

func TestWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)
	w.Simple("OK")
	w.Error("ERR bad\r\nthing")
	w.Integer(-42)
	w.Bulk([]byte("hi"))
	w.Null()
	w.Array(2)
	w.BulkString("a")
	w.Bulk(nil)
	if err := w.Flush(); err != nil {
		t.Fatal(err)
	}
	want := "+OK\r\n-ERR bad  thing\r\n:-42\r\n$2\r\nhi\r\n$-1\r\n*2\r\n$1\r\na\r\n$0\r\n\r\n"
	if got := buf.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
}

func FuzzReadCommand(f *testing.F) {
	seeds := []string{
		"*1\r\n$4\r\nPING\r\n",
		"PING\r\n",
		"*2\r\n$3\r\nGET\r\n$1\r\nk\r\n",
		"*-1\r\n",
		"*1\r\n$5\r\nab",
		"$5\r\n",
	}
	for _, s := range seeds {
		f.Add([]byte(s))
	}
	f.Fuzz(func(t *testing.T, data []byte) {
		r := NewReader(bytes.NewReader(data), 1<<16)
		for range 64 {
			if _, err := r.ReadCommand(); err != nil {
				return
			}
		}
	})
}
