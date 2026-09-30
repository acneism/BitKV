package main

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

var errNotSent = errors.New("request not sent")

type respError string

func (e respError) Error() string { return string(e) }

type respConn struct {
	net.Conn
	r   *bufio.Reader
	gen int
}

type respPool struct {
	mu   sync.Mutex
	idle map[string][]*respConn
	gen  map[string]int
}

func newRespPool() *respPool {
	return &respPool{idle: map[string][]*respConn{}, gen: map[string]int{}}
}

func (p *respPool) get(addr string) (*respConn, error) {
	p.mu.Lock()
	gen := p.gen[addr]
	if cs := p.idle[addr]; len(cs) > 0 {
		c := cs[len(cs)-1]
		p.idle[addr] = cs[:len(cs)-1]
		p.mu.Unlock()
		return c, nil
	}
	p.mu.Unlock()
	c, err := net.DialTimeout("tcp", addr, time.Second)
	if err != nil {
		return nil, errNotSent
	}
	return &respConn{Conn: c, r: bufio.NewReader(c), gen: gen}, nil
}

func (p *respPool) put(addr string, c *respConn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if c.gen == p.gen[addr] && len(p.idle[addr]) < 16 {
		p.idle[addr] = append(p.idle[addr], c)
		return
	}
	c.Close()
}

func (p *respPool) drop(addr string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.gen[addr]++
	for _, c := range p.idle[addr] {
		c.Close()
	}
	delete(p.idle, addr)
}

func (p *respPool) close() {
	p.mu.Lock()
	defer p.mu.Unlock()
	for addr := range p.idle {
		for _, c := range p.idle[addr] {
			c.Close()
		}
	}
	clear(p.idle)
}

func (p *respPool) do(addr string, cmds ...[]string) ([]any, error) {
	return p.doWithin(addr, 5*time.Second, cmds...)
}

func (p *respPool) doWithin(addr string, timeout time.Duration, cmds ...[]string) ([]any, error) {
	c, err := p.get(addr)
	if err != nil {
		return nil, err
	}
	_ = c.SetDeadline(time.Now().Add(timeout))
	var b strings.Builder
	for _, args := range cmds {
		fmt.Fprintf(&b, "*%d\r\n", len(args))
		for _, a := range args {
			fmt.Fprintf(&b, "$%d\r\n%s\r\n", len(a), a)
		}
	}
	if _, err := io.WriteString(c, b.String()); err != nil {
		c.Close()
		return nil, err
	}
	replies := make([]any, len(cmds))
	for i := range cmds {
		if replies[i], err = readReply(c.r); err != nil {
			c.Close()
			return nil, err
		}
	}
	p.put(addr, c)
	return replies, nil
}

func (p *respPool) one(addr string, args ...string) (any, error) {
	return p.oneWithin(addr, 5*time.Second, args...)
}

func (p *respPool) oneWithin(addr string, timeout time.Duration, args ...string) (any, error) {
	replies, err := p.doWithin(addr, timeout, args)
	if err != nil {
		return nil, err
	}
	if e, ok := replies[0].(respError); ok {
		return nil, e
	}
	return replies[0], nil
}

func readReply(r *bufio.Reader) (any, error) {
	line, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return nil, errors.New("empty reply")
	}
	body := line[1:]
	switch line[0] {
	case '+':
		return body, nil
	case '-':
		return respError(body), nil
	case ':':
		return strconv.ParseInt(body, 10, 64)
	case '$':
		n, err := strconv.Atoi(body)
		if err != nil || n < 0 {
			return nil, err
		}
		buf := make([]byte, n+2)
		if _, err := io.ReadFull(r, buf); err != nil {
			return nil, err
		}
		return string(buf[:n]), nil
	case '*':
		n, err := strconv.Atoi(body)
		if err != nil || n < 0 {
			return nil, err
		}
		items := make([]any, n)
		for i := range items {
			if items[i], err = readReply(r); err != nil {
				return nil, err
			}
		}
		return items, nil
	}
	return nil, fmt.Errorf("unexpected reply %q", line)
}
