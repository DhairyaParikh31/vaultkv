// Package memtable implements the in-memory sorted data structure
// that buffers all recent writes before they are flushed to SSTables.
//
// VaultKV uses a skip list as the underlying data structure — the same
// choice made by LevelDB, RocksDB, and Apache Cassandra — because it
// provides O(log n) insert and lookup with simpler concurrent access
// patterns than a Red-Black tree.
//
// Skip List Structure (4 levels shown, max 12 in VaultKV):
//
//	L3: head ──────────────────────────────── [g] ──── tail
//	L2: head ──────── [b] ──────── [e] ─────── [g] ──── tail
//	L1: head ──────── [b] ── [c] ── [e] ─────── [g] ──── tail
//	L0: head ── [a] ── [b] ── [c] ── [d] ── [e] ── [f] ── [g] ── tail
//
// Search for "f": start at L3, drop down levels, traverse forward
// only when next key <= target. Expected comparisons: O(log n).
package memtable

import (
	"bytes"
	"math/rand"
	"sync"
)

const (
	// maxLevel is the maximum number of levels in the skip list.
	// Justified by: log_{1/p}(n) = log_4(10^7) ≈ 11.6 for 10M keys.
	maxLevel = 12

	// probability is the node promotion probability.
	// Each key is promoted to the next level with p = 0.25.
	// Expected node height = 1/(1-p) = 1.333 levels (memory efficient).
	probability = 0.25
)

// node is a single element in the skip list.
// Each node holds a key, value, a deleted flag (tombstone), and
// a slice of forward pointers — one per level the node appears in.
type node struct {
	key     []byte
	value   []byte
	deleted bool     // true = tombstone (DELETE record)
	forward []*node  // forward[i] = next node at level i
}

// newNode allocates a node with the given level count.
func newNode(key, value []byte, deleted bool, level int) *node {
	return &node{
		key:     key,
		value:   value,
		deleted: deleted,
		forward: make([]*node, level),
	}
}

// skipList is a probabilistic sorted data structure.
// It is NOT safe for concurrent use on its own — the MemTable
// wraps it with a sync.RWMutex.
type skipList struct {
	head     *node  // sentinel head node (no key/value)
	level    int    // current highest active level (1-indexed)
	length   int    // number of entries (including tombstones)
	byteSize int64  // estimated byte size of all keys + values
	rng      *rand.Rand
}

// newSkipList initialises an empty skip list.
func newSkipList() *skipList {
	head := newNode(nil, nil, false, maxLevel)
	return &skipList{
		head:  head,
		level: 1,
		rng:   rand.New(rand.NewSource(42)),
	}
}

// randomLevel generates a random level for a new node using
// the geometric distribution with p = 0.25.
//
// P(level = i) = p^(i-1) * (1-p)
// Expected level = 1/(1-p) = 1.333
func (s *skipList) randomLevel() int {
	lvl := 1
	for lvl < maxLevel && s.rng.Float64() < probability {
		lvl++
	}
	return lvl
}

// set inserts or updates a key in the skip list.
// If the key already exists, its value is overwritten.
// Complexity: O(log n) average.
func (s *skipList) set(key, value []byte) {
	// update[i] holds the rightmost node at level i whose key < target key.
	// After traversal, update[i].forward[i] is where we insert.
	update := make([]*node, maxLevel)
	curr := s.head

	// Traverse from the highest level down to level 0.
	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil &&
			bytes.Compare(curr.forward[i].key, key) < 0 {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	// Check if key already exists at level 0.
	existing := curr.forward[0]
	if existing != nil && bytes.Equal(existing.key, key) {
		// Update in place — adjust byte size delta.
		s.byteSize -= int64(len(existing.value))
		s.byteSize += int64(len(value))
		existing.value = value
		existing.deleted = false
		return
	}

	// Key does not exist — insert a new node.
	lvl := s.randomLevel()
	if lvl > s.level {
		// New node reaches levels above current maximum.
		// Point those levels' update entries to head.
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	n := newNode(key, value, false, lvl)
	for i := 0; i < lvl; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}

	s.length++
	s.byteSize += int64(len(key)) + int64(len(value))
}

// delete inserts a tombstone for the given key.
// A tombstone signals that the key has been deleted — it prevents
// older values in lower SSTables from being returned on reads.
// Complexity: O(log n) average.
func (s *skipList) delete(key []byte) {
	update := make([]*node, maxLevel)
	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil &&
			bytes.Compare(curr.forward[i].key, key) < 0 {
			curr = curr.forward[i]
		}
		update[i] = curr
	}

	existing := curr.forward[0]
	if existing != nil && bytes.Equal(existing.key, key) {
		// Key exists — convert to tombstone.
		s.byteSize -= int64(len(existing.value))
		existing.value = nil
		existing.deleted = true
		return
	}

	// Key does not exist — insert a tombstone node anyway.
	// This is necessary because a DELETE may arrive before a SET
	// (e.g. during WAL replay or out-of-order operations).
	lvl := s.randomLevel()
	if lvl > s.level {
		for i := s.level; i < lvl; i++ {
			update[i] = s.head
		}
		s.level = lvl
	}

	n := newNode(key, nil, true, lvl)
	for i := 0; i < lvl; i++ {
		n.forward[i] = update[i].forward[i]
		update[i].forward[i] = n
	}

	s.length++
	s.byteSize += int64(len(key))
}

// get returns the value for a key, whether it is a tombstone,
// and whether the key was found at all.
//
// Returns:
//
//	value, false, true  → key found, not deleted
//	nil,   true,  true  → key found but tombstoned (deleted)
//	nil,   false, false → key not found in this skip list
//
// Complexity: O(log n) average.
func (s *skipList) get(key []byte) (value []byte, deleted bool, found bool) {
	curr := s.head

	for i := s.level - 1; i >= 0; i-- {
		for curr.forward[i] != nil &&
			bytes.Compare(curr.forward[i].key, key) < 0 {
			curr = curr.forward[i]
		}
	}

	curr = curr.forward[0]
	if curr == nil || !bytes.Equal(curr.key, key) {
		return nil, false, false
	}

	return curr.value, curr.deleted, true
}

// Iterator provides ordered iteration over all entries in the skip list.
// It iterates at level 0 — the full sorted linked list.
type Iterator struct {
	curr *node
	mu   *sync.RWMutex // held as RLock during iteration
}

// newIterator returns an Iterator positioned before the first entry.
// The caller must call Close() when done to release the read lock.
func (s *skipList) newIterator(mu *sync.RWMutex) *Iterator {
	mu.RLock()
	return &Iterator{
		curr: s.head,
		mu:   mu,
	}
}

// Valid returns true if the iterator points to a valid entry.
func (it *Iterator) Valid() bool {
	return it.curr.forward[0] != nil
}

// Next advances the iterator to the next entry.
func (it *Iterator) Next() {
	if it.curr.forward[0] != nil {
		it.curr = it.curr.forward[0]
	}
}

// Key returns the key at the current position.
// Do not modify the returned slice.
func (it *Iterator) Key() []byte {
	return it.curr.forward[0].key
}

// Value returns the value at the current position.
// Returns nil for tombstone entries (check IsDeleted first).
func (it *Iterator) Value() []byte {
	return it.curr.forward[0].value
}

// IsDeleted returns true if the current entry is a tombstone.
func (it *Iterator) IsDeleted() bool {
	return it.curr.forward[0].deleted
}

// Close releases the read lock held during iteration.
// Must be called when the caller is done with the iterator.
func (it *Iterator) Close() {
	if it.mu != nil {
		it.mu.RUnlock()
		it.mu = nil
	}
}