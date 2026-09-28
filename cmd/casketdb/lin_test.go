package main

import (
	"errors"
	"flag"
	"fmt"
	"math"
	"math/rand/v2"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/anishathalye/porcupine"
)

var (
	faultDuration  = flag.Duration("fault.duration", 0, "load duration of TestFaults; 0 skips it")
	memberDuration = flag.Duration("member.duration", 0, "load duration of TestMembershipChanges; 0 skips it")
	faultOut       = flag.String("fault.out", "", "file for the Porcupine visualization when the check fails")
)

const keys = 50

type kvInput struct {
	op    byte
	key   string
	value int64
}

type kvOutput struct {
	exists  bool
	value   int64
	unknown bool
}

type kvState struct {
	exists bool
	value  int64
}

var kvModel = porcupine.Model{
	Partition: func(history []porcupine.Operation) [][]porcupine.Operation {
		byKey := map[string][]porcupine.Operation{}
		for _, op := range history {
			k := op.Input.(kvInput).key
			byKey[k] = append(byKey[k], op)
		}
		var out [][]porcupine.Operation
		for _, ops := range byKey {
			out = append(out, ops)
		}
		return out
	},
	Init: func() any { return kvState{} },
	Step: func(state, input, output any) (bool, any) {
		st, in, out := state.(kvState), input.(kvInput), output.(kvOutput)
		read := out.exists == st.exists && (!st.exists || out.value == st.value)
		switch in.op {
		case 'g':
			return read, st
		case 's':
			return true, kvState{exists: true, value: in.value}
		case 'i':
			next := kvState{exists: true, value: st.value + 1}
			return out.unknown || out.value == next.value, next
		case 'd':
			return out.unknown || out.exists == st.exists, kvState{}
		}
		return out.unknown || read, kvState{exists: true, value: in.value}
	},
	DescribeOperation: func(input, output any) string {
		in, out := input.(kvInput), output.(kvOutput)
		res := "nil"
		switch {
		case out.unknown:
			res = "?"
		case out.exists:
			res = strconv.FormatInt(out.value, 10)
		}
		switch in.op {
		case 'g':
			return fmt.Sprintf("get(%s) -> %s", in.key, res)
		case 's':
			return fmt.Sprintf("set(%s, %d)", in.key, in.value)
		case 'i':
			return fmt.Sprintf("incr(%s) -> %s", in.key, res)
		case 'd':
			return fmt.Sprintf("del(%s) -> existed %v", in.key, out.exists)
		}
		return fmt.Sprintf("multi(get(%s) -> %s, set %d)", in.key, res, in.value)
	},
}

type outcome int

const (
	done outcome = iota
	retry
	unknown
)

func writeFailed(err error) outcome {
	var re respError
	switch {
	case errors.Is(err, errNotSent):
		return retry
	case errors.As(err, &re) && strings.HasPrefix(string(re), "READONLY "):
		return retry
	}
	return unknown
}

func bulkValue(reply any) (kvOutput, bool) {
	if reply == nil {
		return kvOutput{}, true
	}
	s, ok := reply.(string)
	if !ok {
		return kvOutput{}, false
	}
	n, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		n = math.MinInt64
	}
	return kvOutput{exists: true, value: n}, true
}

func (h *harness) execute(addr string, in kvInput) (kvOutput, outcome) {
	switch in.op {
	case 'g':
		reply, err := h.pool.one(addr, "GET", in.key)
		if err != nil {
			return kvOutput{}, retry
		}
		if out, ok := bulkValue(reply); ok {
			return out, done
		}
		return kvOutput{}, retry
	case 's':
		if _, err := h.pool.one(addr, "SET", in.key, strconv.FormatInt(in.value, 10)); err != nil {
			return kvOutput{}, writeFailed(err)
		}
		return kvOutput{}, done
	case 'i':
		reply, err := h.pool.one(addr, "INCR", in.key)
		if err != nil {
			return kvOutput{}, writeFailed(err)
		}
		n, _ := reply.(int64)
		return kvOutput{exists: true, value: n}, done
	case 'd':
		reply, err := h.pool.one(addr, "DEL", in.key)
		if err != nil {
			return kvOutput{}, writeFailed(err)
		}
		return kvOutput{exists: reply == int64(1)}, done
	}
	replies, err := h.pool.do(addr, []string{"MULTI"}, []string{"GET", in.key}, []string{"SET", in.key, strconv.FormatInt(in.value, 10)}, []string{"EXEC"})
	if err != nil {
		return kvOutput{}, writeFailed(err)
	}
	if re, ok := replies[3].(respError); ok {
		return kvOutput{}, writeFailed(re)
	}
	results, ok := replies[3].([]any)
	if !ok || len(results) != 2 || results[1] != "OK" {
		return kvOutput{}, unknown
	}
	out, ok := bulkValue(results[0])
	if !ok {
		return kvOutput{}, unknown
	}
	return out, done
}

func randomInput(client, seq int) kvInput {
	in := kvInput{key: fmt.Sprintf("k%d", rand.IntN(keys)), value: int64(client*1_000_000 + seq)}
	switch r := rand.IntN(20); {
	case r < 9:
		in.op = 'g'
	case r < 13:
		in.op = 's'
	case r < 16:
		in.op = 'i'
	case r < 17:
		in.op = 'd'
	default:
		in.op = 'm'
	}
	return in
}

func (h *harness) linearizability(d time.Duration, nemesis func(until time.Time)) {
	t := h.t
	start := time.Now()
	now := func() int64 { return int64(time.Since(start)) }
	deadline := start.Add(d)
	var mu sync.Mutex
	var history []porcupine.Operation
	var clients, completed, unknowns atomic.Int64
	record := func(op porcupine.Operation) {
		mu.Lock()
		history = append(history, op)
		mu.Unlock()
	}
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := int(clients.Add(1) - 1)
			writer := h.pick()
			for seq := 0; time.Now().Before(deadline); seq++ {
				in := randomInput(client, seq)
				target := writer
				if in.op == 'g' {
					target = h.pick()
				}
				call := now()
				out, res := h.execute(target, in)
				ret := now()
				switch res {
				case done:
					record(porcupine.Operation{ClientId: client, Input: in, Call: call, Output: out, Return: ret})
					completed.Add(1)
				case unknown:
					record(porcupine.Operation{ClientId: client, Input: in, Call: call, Output: kvOutput{unknown: true}, Return: math.MaxInt64})
					unknowns.Add(1)
					client = int(clients.Add(1) - 1)
					writer = h.pick()
				default:
					if in.op == 'g' {
						continue
					}
					if writer = h.leaderOf(h.pick()); writer == "" {
						writer = h.pick()
						time.Sleep(20 * time.Millisecond)
					}
				}
			}
		}()
	}
	nemesis(deadline)
	wg.Wait()

	final := int(clients.Add(1) - 1)
	finalStart := time.Now()
	for k := range keys {
		in := kvInput{op: 'g', key: fmt.Sprintf("k%d", k)}
		for {
			call := now()
			out, res := h.execute(h.pick(), in)
			if res == done {
				record(porcupine.Operation{ClientId: final, Input: in, Call: call, Output: out, Return: now()})
				break
			}
			if time.Since(finalStart) > time.Minute {
				for _, p := range h.live() {
					info := h.replication(p.client)
					t.Logf("%s running=%v state=%s leader=%s membership=%s applied=%s", p.id, p.cmd != nil,
						info["raft_state"], info["raft_leader_id"], info["raft_membership"], info["raft_applied_index"])
				}
				t.Fatalf("the cluster did not answer reads within a minute after the faults stopped; logs in %s", h.dir)
			}
			time.Sleep(20 * time.Millisecond)
		}
	}

	checkStart := time.Now()
	res, info := porcupine.CheckOperationsVerbose(kvModel, history, 5*time.Minute)
	t.Logf("%d operations completed, %d with unknown outcome; checked in %v: %s", completed.Load(), unknowns.Load(), time.Since(checkStart).Round(time.Millisecond), res)
	if completed.Load() == 0 {
		t.Fatal("no operation completed")
	}
	if res != porcupine.Ok {
		path := *faultOut
		if path == "" {
			path = filepath.Join(h.dir, "linearizability.html")
		}
		if err := porcupine.VisualizePath(kvModel, info, path); err != nil {
			t.Log(err)
		}
		h.dumpIllegalKeys(history, start)
		t.Fatalf("history is %s; visualization in %s, node logs and illegal-*.txt in %s", res, path, h.dir)
	}
}

func (h *harness) dumpIllegalKeys(history []porcupine.Operation, start time.Time) {
	for _, ops := range kvModel.Partition(history) {
		if porcupine.CheckOperations(kvModel, ops) {
			continue
		}
		slices.SortFunc(ops, func(a, b porcupine.Operation) int { return int(a.Call - b.Call) })
		var b strings.Builder
		for _, op := range ops {
			ret := "never"
			if op.Return != math.MaxInt64 {
				ret = time.Duration(op.Return).String()
			}
			fmt.Fprintf(&b, "%-14v %-14s client %-4d %s\n", time.Duration(op.Call), ret, op.ClientId, kvModel.DescribeOperation(op.Input, op.Output))
		}
		key := ops[0].Input.(kvInput).key
		if err := os.WriteFile(filepath.Join(h.dir, "illegal-"+key+".txt"), []byte(b.String()), 0o600); err != nil {
			h.t.Log(err)
		}
		h.t.Logf("key %s is not linearizable; started at %s", key, start.Format(time.RFC3339Nano))
	}
}

func TestFaults(t *testing.T) {
	if *faultDuration == 0 {
		t.Skip("run with -fault.duration=D")
	}
	h := newHarness(t)
	h.linearizability(*faultDuration, h.faults)
	s := h.stats
	t.Logf("%d kill -9, %d partitions, %d of %d leadership transfers done", s.kills, s.partitions, s.transferred, s.transfers)
	if s.kills == 0 || s.partitions == 0 || s.transfers == 0 {
		t.Fatal("the run was too short to inject every kind of fault")
	}
}

func TestMembershipChanges(t *testing.T) {
	if *memberDuration == 0 {
		t.Skip("run with -member.duration=D")
	}
	h := newHarness(t)
	h.linearizability(*memberDuration, h.membership)
	var final []string
	if l := h.leaderProc(); l != nil {
		final = h.voters(l.client)
		slices.Sort(final)
	}
	s := h.stats
	t.Logf("%d members added (%d failed), %d removed (%d failed); voters at the end: %v", s.added, s.addFailed, s.removed, s.remFailed, final)
	if s.added == 0 || s.removed == 0 {
		t.Fatal("membership did not change")
	}
}
