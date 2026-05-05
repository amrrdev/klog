package memtable

import (
	"math/rand"
	"sync"
)

const (
	// maxLevel is the maximum number of levels in the skip list.
	// log₂(65536) = 16, so this handles up to ~65,000 entries efficiently.
	// RocksDB uses 12. Redis uses 32. 16 is reasonable for a memtable
	// that flushes at a few MB.
	maxLevel = 16

	// probability is the coin-flip probability for level promotion.
	// 0.5 gives the classic "halving at each level" distribution.
	// Some implementations use 0.25 (used by Redis) for a shallower,
	// wider structure that is faster in practice due to cache locality.
	probability = 0.5
)

// node is a single element in the skip list.
// forward[i] is the next node at level i.
// forward[0] is the standard linked list — every node appears here.
// forward[k] skips over all nodes that do not participate in level k.
type node struct {
	key     string
	value   []byte
	deleted bool
	forward []*node // len(forward) == this node's level count
}

// newNode allocates a node with the given level count.
// level is determined at insertion time by randomLevel().
func newNode(key string, value []byte, level int) *node {
	return &node{
		key:     key,
		value:   value,
		forward: make([]*node, level),
	}
}

// randomLevel determines how many levels a new node participates in.
// It flips a biased coin until it gets tails or hits maxLevel.
// This is the core of why skip lists work without explicit rebalancing.
func randomLevel() int {
	level := 1
	for level < maxLevel && rand.Float64() < probability {
		level++
	}
	return level
}

type SkipList struct {
	mu     sync.RWMutex
	head   *node // sentinel head node
	level  int   // current highest level in use
	count  int   // total nodes including tombstones, used for Iter capacity
	length int   // number of live (non-deleted) keys
	size   int64 // approximate byte size of all live keys and values
}

func NewSkipList() *SkipList {
	return &SkipList{
		head: newNode("", nil, maxLevel),
	}
}

// Set inserts or updates a key. If the key already exists, its value
// is replaced. Deleted keys (tombstones) are overwritten with the new value.
func (sl *SkipList) Set(key string, value []byte) {
	sl.mu.Lock()
	defer sl.mu.Unlock()

	// update[i] is the rightmost node at level i whose key < key.
	// After inserting the new node, update[i].forward[i] must point to it.
	update := make([]*node, maxLevel)

	current := sl.head

	for i := sl.level - 1; i >= 0; i-- {
		for current.forward[i] != nil && current.forward[i].key < key {
			current = current.forward[i]
		}
		update[i] = current
	}

	// Check if the key already exists at level 0.
	existing := update[0].forward[0]
	if existing != nil && existing.key == key {
		if existing.deleted {
			existing.value = value
			existing.deleted = false
			sl.length++
			sl.size += int64(len(key) + len(value))
		} else {
			sl.size += int64(len(value) - len(existing.value))
			existing.value = value
		}
		return
	}

	level := randomLevel()

	// If the new node's level exceeds the current max level, initialize
	// the extra levels in update[] to point to head. The head node acts
	// as the left boundary at every level.
	if level > sl.level {
		for i := sl.level; i < level; i++ {
			update[i] = sl.head
		}
		sl.level = level
	}

	n := newNode(key, value, level)
	for i := range level {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}
	sl.length++
	sl.count++
	sl.size += int64(len(key) + len(value))
}
