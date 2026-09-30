package server

import (
	"bufio"
	"io"
	"net"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func benchConn(b *testing.B, addr string) (net.Conn, *bufio.Reader) {
	b.Helper()
	conn, r, err := dialBench(addr)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { conn.Close() })
	return conn, r
}

func dialBench(addr string) (net.Conn, *bufio.Reader, error) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		return nil, nil, err
	}
	_ = conn.SetDeadline(time.Now().Add(5 * time.Minute))
	return conn, bufio.NewReader(conn), nil
}

func readLines(r *bufio.Reader, n int) error {
	for range n {
		line, err := r.ReadString('\n')
		if err != nil {
			return err
		}
		if line[0] == '-' {
			return io.ErrUnexpectedEOF
		}
		if line[0] == '$' && line != "$-1\r\n" {
			if _, err := r.ReadString('\n'); err != nil {
				return err
			}
		}
	}
	return nil
}

const benchDepth = 64

func pipelinePayload(cmd func(i int) string) []byte {
	var batch strings.Builder
	for i := range benchDepth {
		batch.WriteString(cmd(i))
	}
	return []byte(batch.String())
}

func benchPipeline(b *testing.B, warm, cmd func(i int) string) {
	srv, db, addr := startServer(b, b.TempDir())
	b.Cleanup(func() { stopServer(b, srv, db) })
	conn, r := benchConn(b, addr)
	const depth = benchDepth
	if warm != nil {
		if _, err := conn.Write(pipelinePayload(warm)); err != nil {
			b.Fatal(err)
		}
		if err := readLines(r, depth); err != nil {
			b.Fatal(err)
		}
	}
	payload := pipelinePayload(cmd)
	b.ResetTimer()
	for done := 0; done < b.N; done += depth {
		if _, err := conn.Write(payload); err != nil {
			b.Fatal(err)
		}
		if err := readLines(r, depth); err != nil {
			b.Fatal(err)
		}
	}
}

func setCmd(i int) string {
	return encode("SET", "key:"+strconv.Itoa(i), strings.Repeat("v", 100))
}

func BenchmarkServerPipelinedSet(b *testing.B) {
	benchPipeline(b, nil, setCmd)
}

func BenchmarkServerPipelinedGet(b *testing.B) {
	benchPipeline(b, setCmd, func(i int) string { return encode("GET", "key:"+strconv.Itoa(i)) })
}

func BenchmarkServerPipelinedIncr(b *testing.B) {
	benchPipeline(b, nil, func(int) string { return encode("INCR", "counter") })
}

func BenchmarkServerParallelPipelinedSet(b *testing.B) {
	srv, db, addr := startServer(b, b.TempDir())
	b.Cleanup(func() { stopServer(b, srv, db) })
	var worker atomic.Int64
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, r, err := dialBench(addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()
		id := worker.Add(1)
		payload := pipelinePayload(func(i int) string {
			return encode("SET", "w"+strconv.FormatInt(id, 10)+":"+strconv.Itoa(i), strings.Repeat("v", 100))
		})
		pending := 0
		for pb.Next() {
			pending++
			if pending < benchDepth {
				continue
			}
			pending = 0
			if _, err := conn.Write(payload); err != nil {
				b.Error(err)
				return
			}
			if err := readLines(r, benchDepth); err != nil {
				b.Error(err)
				return
			}
		}
	})
}

func BenchmarkServerParallelSet(b *testing.B) {
	srv, db, addr := startServer(b, b.TempDir())
	b.Cleanup(func() { stopServer(b, srv, db) })
	value := strings.Repeat("v", 100)
	b.SetParallelism(8)
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		conn, r, err := dialBench(addr)
		if err != nil {
			b.Error(err)
			return
		}
		defer conn.Close()
		i := 0
		for pb.Next() {
			i++
			if _, err := io.WriteString(conn, encode("SET", "key:"+strconv.Itoa(i%1000), value)); err != nil {
				b.Error(err)
				return
			}
			if err := readLines(r, 1); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
