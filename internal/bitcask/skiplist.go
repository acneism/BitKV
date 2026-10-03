package bitcask

import (
	"bytes"
	"math/rand/v2"
	"slices"
)

const skipMaxLevel = 32

type skipNode struct {
	value  []byte
	member string
	back   *skipNode
	level  []skipLevel
}

type skipLevel struct {
	next *skipNode
	span int
}

type skiplist struct {
	head   *skipNode
	tail   *skipNode
	length int
	level  int
}

func newSkiplist() *skiplist {
	return &skiplist{head: &skipNode{level: make([]skipLevel, skipMaxLevel)}, level: 1}
}

func orderLess(v1 []byte, m1 string, v2 []byte, m2 string) bool {
	if c := bytes.Compare(v1, v2); c != 0 {
		return c < 0
	}
	return m1 < m2
}

func (n *skipNode) step(reverse bool) *skipNode {
	if reverse {
		return n.back
	}
	return n.level[0].next
}

func (sl *skiplist) insert(value []byte, member string) *skipNode {
	var update [skipMaxLevel]*skipNode
	var rank [skipMaxLevel]int
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		if i < sl.level-1 {
			rank[i] = rank[i+1]
		}
		for next := x.level[i].next; next != nil && orderLess(next.value, next.member, value, member); next = x.level[i].next {
			rank[i] += x.level[i].span
			x = next
		}
		update[i] = x
	}
	level := 1
	for level < skipMaxLevel && rand.Uint32()&3 == 0 {
		level++
	}
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			rank[i], update[i] = 0, sl.head
			sl.head.level[i].span = sl.length
		}
		sl.level = level
	}
	x = &skipNode{value: value, member: member, level: make([]skipLevel, level)}
	for i := range level {
		x.level[i].next = update[i].level[i].next
		update[i].level[i].next = x
		x.level[i].span = update[i].level[i].span - (rank[0] - rank[i])
		update[i].level[i].span = rank[0] - rank[i] + 1
	}
	for i := level; i < sl.level; i++ {
		update[i].level[i].span++
	}
	if update[0] != sl.head {
		x.back = update[0]
	}
	if x.level[0].next != nil {
		x.level[0].next.back = x
	} else {
		sl.tail = x
	}
	sl.length++
	return x
}

func (sl *skiplist) delete(value []byte, member string) {
	var update [skipMaxLevel]*skipNode
	x := sl.head
	for i := sl.level - 1; i >= 0; i-- {
		for next := x.level[i].next; next != nil && orderLess(next.value, next.member, value, member); next = x.level[i].next {
			x = next
		}
		update[i] = x
	}
	x = x.level[0].next
	if x == nil || !bytes.Equal(x.value, value) || x.member != member {
		return
	}
	for i := range sl.level {
		if update[i].level[i].next == x {
			update[i].level[i].span += x.level[i].span - 1
			update[i].level[i].next = x.level[i].next
		} else {
			update[i].level[i].span--
		}
	}
	if x.level[0].next != nil {
		x.level[0].next.back = x.back
	} else {
		sl.tail = x.back
	}
	for sl.level > 1 && sl.head.level[sl.level-1].next == nil {
		sl.level--
	}
	sl.length--
}

func (sl *skiplist) count(below func(value []byte, member string) bool) int {
	x, n := sl.head, 0
	for i := sl.level - 1; i >= 0; i-- {
		for x.level[i].next != nil && below(x.level[i].next.value, x.level[i].next.member) {
			n += x.level[i].span
			x = x.level[i].next
		}
	}
	return n
}

func (sl *skiplist) at(i int) *skipNode {
	if i < 0 || i >= sl.length {
		return nil
	}
	x, passed := sl.head, 0
	for l := sl.level - 1; l >= 0; l-- {
		for x.level[l].next != nil && passed+x.level[l].span <= i+1 {
			passed += x.level[l].span
			x = x.level[l].next
		}
		if passed == i+1 {
			return x
		}
	}
	return nil
}

type orderedView struct {
	stored  *skiplist
	changed map[string]pendingOp
	gone    []*skipNode
	ranks   []int
	added   *skiplist
}

func newOrderedView(stored *skiplist, nodes map[string]*skipNode, changed map[string]pendingOp) *orderedView {
	v := &orderedView{stored: stored, changed: changed, added: newSkiplist()}
	for m, op := range changed {
		if n := nodes[m]; n != nil {
			v.gone = append(v.gone, n)
			v.ranks = append(v.ranks, stored.count(func(value []byte, member string) bool {
				return orderLess(value, member, n.value, n.member)
			}))
		}
		if !op.deleted {
			v.added.insert(op.value, m)
		}
	}
	slices.Sort(v.ranks)
	return v
}

func (v *orderedView) length() int {
	return v.stored.length - len(v.gone) + v.added.length
}

func (v *orderedView) count(below func(value []byte, member string) bool) int {
	n := v.stored.count(below) + v.added.count(below)
	for _, g := range v.gone {
		if below(g.value, g.member) {
			n--
		}
	}
	return n
}

func (v *orderedView) storedAt(i int) *skipNode {
	if i < 0 {
		return nil
	}
	for _, r := range v.ranks {
		if r > i {
			break
		}
		i++
	}
	return v.stored.at(i)
}

func (v *orderedView) walk(from int, reverse bool, fn func(member string, value []byte) bool) {
	if from < 0 || from >= v.length() {
		return
	}
	before := 0
	for a := v.added.at(0); a != nil; a = a.level[0].next {
		rank := v.count(func(value []byte, member string) bool {
			return orderLess(value, member, a.value, a.member)
		})
		if rank > from || rank == from && !reverse {
			break
		}
		before++
	}
	x, y := v.storedAt(from-before), v.added.at(before)
	if reverse {
		y = v.added.at(before - 1)
	}
	for {
		for x != nil && v.isChanged(x.member) {
			x = x.step(reverse)
		}
		if x == nil && y == nil {
			return
		}
		if y == nil || x != nil && orderLess(x.value, x.member, y.value, y.member) != reverse {
			if !fn(x.member, x.value) {
				return
			}
			x = x.step(reverse)
		} else {
			if !fn(y.member, y.value) {
				return
			}
			y = y.step(reverse)
		}
	}
}

func (v *orderedView) isChanged(member string) bool {
	_, ok := v.changed[member]
	return ok
}
