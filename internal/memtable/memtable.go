package memtable

import (
	"sync"
)

// Entry represents a single key-value pair returned from the MemTable.
// Deleted is true when the entry is a tombstone (DELETE record).
type Entry struct {
	Key     []byte
	Value   []byte
	Deleted bool
}

// MemTable is a thread-safe in-memory sorted buffer for recent writes.
// It wraps a skip list with a sync.RWMutex:
//   - Multiple concurrent readers via RLock
//   - Single writer via Lock (blocks all readers during insert)
//
// When ByteSize() exceeds the configured threshold, the MemTable
// should be rotated: converted to immutable and flushed to an SSTable.
type MemTable struct {
	mu   sync.RWMutex
	list *skipList
}

// New returns an empty, ready-to-use MemTable.
func New() *MemTable {
	return &MemTable{
		list: newSkipList(),
	}
}

// Set inserts or updates a key-value pair.
// If the key already exists, its value is overwritten.
// Complexity: O(log n) average.
func (m *MemTable) Set(key, value []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.set(key, value)
}

// Delete inserts a tombstone for the given key.
// A tombstone prevents older values in SSTables from being visible.
// Complexity: O(log n) average.
func (m *MemTable) Delete(key []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.list.delete(key)
}

// Get returns the entry for a key.
//
// Return values:
//
//	entry, true  → key found (check entry.Deleted for tombstone)
//	zero, false  → key not present in this MemTable
//
// Complexity: O(log n) average.
func (m *MemTable) Get(key []byte) (Entry, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()

	value, deleted, found := m.list.get(key)
	if !found {
		return Entry{}, false
	}
	return Entry{
		Key:     key,
		Value:   value,
		Deleted: deleted,
	}, true
}

// NewIterator returns a sorted iterator over all entries.
// The iterator holds a read lock — call Close() when done.
//
// Typical usage:
//
//	it := mem.NewIterator()
//	for ; it.Valid(); it.Next() {
//	    key := it.Key()
//	    val := it.Value()
//	}
//	it.Close()
func (m *MemTable) NewIterator() *Iterator {
	return m.list.newIterator(&m.mu)
}

// ByteSize returns the estimated memory used by all keys and values.
// Used by the storage engine to decide when to rotate the MemTable.
func (m *MemTable) ByteSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.byteSize
}

// Len returns the number of entries (including tombstones).
func (m *MemTable) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return m.list.length
}