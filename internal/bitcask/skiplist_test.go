package bitcask

import (
	"bytes"
	"fmt"
	"math/rand/v2"
	"slices"
	"strings"
	"testing"
)

type skipItem struct {
	value  []byte
	member string
}

func compareItems(a, b skipItem) int {
	if c := bytes.Compare(a.value, b.value); c != 0 {
		return c
	}
	return strings.Compare(a.member, b.member)
}

func TestSkiplistModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(7, 8))
	sl := newSkiplist()
	var model []skipItem
	for step := range 20000 {
		item := skipItem{value: []byte{byte(rng.IntN(16))}, member: fmt.Sprint(rng.IntN(300))}
		i, found := slices.BinarySearchFunc(model, item, compareItems)
		if rng.IntN(3) == 0 {
			sl.delete(item.value, item.member)
			if found {
				model = slices.Delete(model, i, i+1)
			}
		} else if !found {
			sl.insert(item.value, item.member)
			model = slices.Insert(model, i, item)
		}
		if step%500 != 0 {
			continue
		}
		if sl.length != len(model) {
			t.Fatalf("step %d: length %d, model %d", step, sl.length, len(model))
		}
		for i, want := range model {
			if n := sl.at(i); n == nil || !bytes.Equal(n.value, want.value) || n.member != want.member {
				t.Fatalf("step %d: at(%d) = %+v, want %+v", step, i, n, want)
			}
		}
		for x, i := sl.tail, len(model)-1; x != nil; x, i = x.back, i-1 {
			if x.member != model[i].member {
				t.Fatalf("step %d: backward walk at %d = %s, want %s", step, i, x.member, model[i].member)
			}
		}
		pivot := []byte{byte(rng.IntN(16))}
		want, _ := slices.BinarySearchFunc(model, skipItem{value: pivot}, compareItems)
		if got := sl.count(func(v []byte, _ string) bool { return bytes.Compare(v, pivot) < 0 }); got != want {
			t.Fatalf("step %d: count below %v = %d, want %d", step, pivot, got, want)
		}
	}
	if sl.at(-1) != nil || sl.at(len(model)) != nil {
		t.Fatal("at outside the list returned a node")
	}
}

func TestOrderedViewModel(t *testing.T) {
	rng := rand.New(rand.NewPCG(9, 10))
	for round := range 300 {
		stored, nodes := newSkiplist(), make(map[string]*skipNode)
		values := make(map[string][]byte)
		for range rng.IntN(60) {
			m, v := fmt.Sprint(rng.IntN(80)), []byte{byte(rng.IntN(8))}
			if n := nodes[m]; n != nil {
				stored.delete(n.value, m)
			}
			nodes[m], values[m] = stored.insert(v, m), v
		}
		changed := make(map[string]pendingOp)
		for range rng.IntN(12) {
			m := fmt.Sprint(rng.IntN(80))
			if rng.IntN(3) == 0 {
				changed[m] = pendingOp{deleted: true}
				delete(values, m)
			} else {
				v := []byte{byte(rng.IntN(8))}
				changed[m], values[m] = pendingOp{value: v}, v
			}
		}
		var model []skipItem
		for m, v := range values {
			model = append(model, skipItem{v, m})
		}
		slices.SortFunc(model, compareItems)
		view := newOrderedView(stored, nodes, changed, false)
		if view.length() != len(model) {
			t.Fatalf("round %d: length %d, model %d", round, view.length(), len(model))
		}
		for i := range model {
			for _, reverse := range []bool{false, true} {
				var got []skipItem
				view.walk(i, reverse, func(m string, v []byte) bool {
					got = append(got, skipItem{v, m})
					return true
				})
				want := slices.Clone(model[i:])
				if reverse {
					want = slices.Clone(model[:i+1])
					slices.Reverse(want)
				}
				if !slices.EqualFunc(got, want, func(a, b skipItem) bool { return compareItems(a, b) == 0 }) {
					t.Fatalf("round %d: walk from %d reverse %v = %v, want %v", round, i, reverse, got, want)
				}
			}
		}
		pivot := []byte{byte(rng.IntN(9))}
		want, _ := slices.BinarySearchFunc(model, skipItem{value: pivot}, compareItems)
		if got := view.count(func(v []byte, _ string) bool { return bytes.Compare(v, pivot) < 0 }); got != want {
			t.Fatalf("round %d: count below %v = %d, want %d", round, pivot, got, want)
		}
	}
}
