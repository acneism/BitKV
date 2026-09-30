package main

import (
	"flag"
	"fmt"
	"math/rand/v2"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"
)

var (
	faultBin    = flag.String("fault.bin", "", "prebuilt casketdb binary; built from source when empty")
	faultReads  = flag.String("fault.reads", "linearizable", "-raft-reads of the nodes: linearizable or lease")
	faultNoSync = flag.Bool("fault.nosync", false, "run the nodes with -raft-unsafe-no-fsync")
)

type proc struct {
	id      string
	client  string
	raft    string
	listen  string
	peers   string
	join    bool
	joining bool
	removed bool
	cmd     *exec.Cmd
	out     *os.File
}

type nemesisStats struct {
	kills, partitions                    int
	transfers, transferred               int
	added, addFailed, removed, remFailed int
}

type harness struct {
	t     *testing.T
	bin   string
	dir   string
	peers string
	pool  *respPool
	net   *linkProxy

	mu    sync.Mutex
	procs []*proc
	stats nemesisStats
}

var usedPorts sync.Map

func freePorts(t *testing.T, n int) []string {
	t.Helper()
	var out []string
	for len(out) < n {
		port := 20000 + rand.IntN(12000)
		if _, taken := usedPorts.LoadOrStore(port, true); taken {
			continue
		}
		l, err := net.Listen("tcp", fmt.Sprintf("127.0.0.1:%d", port))
		if err != nil {
			continue
		}
		l.Close()
		out = append(out, l.Addr().String())
	}
	return out
}

func buildBinary(t *testing.T) string {
	t.Helper()
	bin := filepath.Join(t.TempDir(), "casketdb")
	if runtime.GOOS == "windows" {
		bin += ".exe"
	}
	if out, err := exec.Command("go", "build", "-o", bin, ".").CombinedOutput(); err != nil {
		t.Fatalf("build: %v\n%s", err, out)
	}
	return bin
}

func newHarness(t *testing.T) *harness {
	bin := *faultBin
	if bin == "" {
		bin = buildBinary(t)
	}
	dir, err := os.MkdirTemp("", "casketdb-fault-")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if !t.Failed() {
			os.RemoveAll(dir)
		}
	})
	h := &harness{t: t, bin: bin, dir: dir, pool: newRespPool(), net: newLinkProxy(t)}
	t.Cleanup(h.pool.close)
	var peers []string
	for i := range 3 {
		p := h.newProc(fmt.Sprintf("n%d", i+1))
		peers = append(peers, p.id+"="+p.raft)
		h.procs = append(h.procs, p)
	}
	h.peers = strings.Join(peers, ",")
	for _, p := range h.procs {
		h.start(p)
	}
	t.Cleanup(func() {
		h.mu.Lock()
		defer h.mu.Unlock()
		for _, p := range h.procs {
			if p.cmd != nil {
				h.kill(p)
			}
		}
	})
	return h
}

func (h *harness) newProc(id string) *proc {
	ports := freePorts(h.t, 2)
	out, err := os.OpenFile(filepath.Join(h.dir, id+".log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		h.t.Fatal(err)
	}
	h.t.Cleanup(func() { out.Close() })
	return &proc{id: id, client: ports[0], listen: ports[1], raft: h.net.listen(h.t, id, ports[1]), out: out}
}

func (h *harness) start(p *proc) {
	h.t.Helper()
	peers := h.peers
	if p.peers != "" {
		peers = p.peers
	}
	args := []string{
		"-addr", p.client, "-dir", filepath.Join(h.dir, p.id), "-raft-id", p.id, "-raft-peers", peers,
		"-raft-listen", p.listen, "-raft-election-timeout", "200ms", "-raft-reads", *faultReads,
	}
	if *faultNoSync {
		args = append(args, "-raft-unsafe-no-fsync")
	}
	if p.join {
		args = append(args, "-raft-join")
	}
	p.cmd = exec.Command(h.bin, args...)
	p.cmd.Stdout, p.cmd.Stderr = p.out, p.out
	if err := p.cmd.Start(); err != nil {
		h.t.Fatal(err)
	}
}

func (h *harness) kill(p *proc) {
	_ = p.cmd.Process.Kill()
	_ = p.cmd.Wait()
	p.cmd = nil
	h.pool.drop(p.client)
}

func (h *harness) live() []*proc {
	h.mu.Lock()
	defer h.mu.Unlock()
	var out []*proc
	for _, p := range h.procs {
		if !p.removed && !p.joining {
			out = append(out, p)
		}
	}
	return out
}

func (h *harness) pick() string {
	live := h.live()
	return live[rand.IntN(len(live))].client
}

func (h *harness) clientOf(id string) string {
	for _, p := range h.live() {
		if p.id == id {
			return p.client
		}
	}
	return ""
}

func (h *harness) replication(addr string) map[string]string {
	reply, err := h.pool.one(addr, "INFO", "replication")
	if err != nil {
		return nil
	}
	s, _ := reply.(string)
	fields := map[string]string{}
	for _, line := range strings.Split(s, "\r\n") {
		if k, v, ok := strings.Cut(line, ":"); ok {
			fields[k] = v
		}
	}
	return fields
}

func (h *harness) leaderOf(addr string) string {
	return h.clientOf(h.replication(addr)["raft_leader_id"])
}

func (h *harness) leaderProc() *proc {
	for _, p := range h.live() {
		if p.cmd != nil && h.replication(p.client)["raft_state"] == "Leader" {
			return p
		}
	}
	return nil
}

func (h *harness) killRestart() {
	live := h.live()
	victim := live[rand.IntN(len(live))]
	h.mu.Lock()
	h.stats.kills++
	h.kill(victim)
	h.mu.Unlock()
	time.Sleep(time.Duration(100+rand.IntN(700)) * time.Millisecond)
	h.mu.Lock()
	defer h.mu.Unlock()
	if !victim.removed {
		h.start(victim)
	}
}

func (h *harness) partition() {
	h.stats.partitions++
	var ids []string
	for _, p := range h.live() {
		ids = append(ids, p.id)
	}
	if rand.IntN(2) == 0 {
		x := ids[rand.IntN(len(ids))]
		for _, y := range ids {
			if y != x {
				h.net.block(x, y)
				h.net.block(y, x)
			}
		}
		return
	}
	for range 1 + rand.IntN(3) {
		if a, b := ids[rand.IntN(len(ids))], ids[rand.IntN(len(ids))]; a != b {
			h.net.block(a, b)
		}
	}
}

func (h *harness) transfer() {
	l := h.leaderProc()
	if l == nil {
		return
	}
	voters := h.voters(l.client)
	if len(voters) == 0 {
		return
	}
	to := voters[rand.IntN(len(voters))]
	if to == l.id {
		return
	}
	h.stats.transfers++
	if reply, err := h.pool.one(l.client, "RAFT", "TRANSFER", to); err == nil && reply == "OK" {
		h.stats.transferred++
	}
}

func (h *harness) faults(until time.Time) {
	for time.Now().Before(until) {
		time.Sleep(time.Duration(300+rand.IntN(1200)) * time.Millisecond)
		switch rand.IntN(4) {
		case 0:
			h.transfer()
		case 1:
			h.partition()
		case 2:
			h.net.heal()
		default:
			h.killRestart()
		}
	}
	h.net.heal()
}

func (h *harness) voters(addr string) []string {
	reply, err := h.pool.one(addr, "RAFT", "MEMBERS")
	if err != nil {
		return nil
	}
	items, _ := reply.([]any)
	var ids []string
	for _, item := range items {
		if m, ok := item.([]any); ok && len(m) == 3 && m[2] == "voter" {
			ids = append(ids, m[0].(string))
		}
	}
	return ids
}

func (h *harness) changeMembers(args ...string) bool {
	deadline := time.Now().Add(90 * time.Second)
	for time.Now().Before(deadline) {
		l := h.leaderProc()
		if l == nil {
			time.Sleep(50 * time.Millisecond)
			continue
		}
		_, err := h.pool.oneWithin(l.client, time.Minute, append([]string{"RAFT"}, args...)...)
		if err == nil || strings.Contains(fmt.Sprint(err), "invalid configuration change") {
			return true
		}
		time.Sleep(50 * time.Millisecond)
	}
	return false
}

func (h *harness) addMember() {
	h.mu.Lock()
	id := fmt.Sprintf("n%d", len(h.procs)+1)
	h.mu.Unlock()
	p := h.newProc(id)
	peers := []string{p.id + "=" + p.raft}
	for _, q := range h.live() {
		peers = append(peers, q.id+"="+q.raft)
	}
	p.peers, p.join, p.joining = strings.Join(peers, ","), true, true
	h.mu.Lock()
	h.start(p)
	h.procs = append(h.procs, p)
	h.mu.Unlock()
	if h.changeMembers("ADDLEARNER", p.id, p.raft) && h.changeMembers("PROMOTE", p.id) {
		h.mu.Lock()
		p.joining = false
		h.mu.Unlock()
		h.stats.added++
		return
	}
	h.stats.addFailed++
	h.t.Logf("adding %s failed, removing it again", p.id)
	h.changeMembers("REMOVE", p.id)
	h.retire(p.id)
}

func (h *harness) removeMember(voters []string) {
	id := voters[rand.IntN(len(voters))]
	if !h.changeMembers("REMOVE", id) {
		h.stats.remFailed++
		return
	}
	h.stats.removed++
	h.retire(id)
}

func (h *harness) retire(id string) {
	h.mu.Lock()
	defer h.mu.Unlock()
	for _, p := range h.procs {
		if p.id == id && !p.removed {
			p.removed = true
			if p.cmd != nil {
				h.kill(p)
			}
		}
	}
}

func (h *harness) membership(until time.Time) {
	for time.Now().Before(until) {
		time.Sleep(time.Duration(500+rand.IntN(1000)) * time.Millisecond)
		l := h.leaderProc()
		if l == nil {
			continue
		}
		voters := h.voters(l.client)
		switch {
		case len(voters) == 0:
		case len(voters) < 5 && (len(voters) <= 3 || rand.IntN(2) == 0):
			h.addMember()
		case len(voters) > 3:
			h.removeMember(voters)
		}
	}
}
