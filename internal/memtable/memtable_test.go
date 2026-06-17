package memtable

import (
	"fmt"
	"testing"
)

// TestSetAndGet verifies basic set and get operations.
func TestSetAndGet(t *testing.T) {
	m := New()

	m.Set([]byte("name"), []byte("John"))
	m.Set([]byte("city"), []byte("vadodara"))

	entry, found := m.Get([]byte("name"))
	if !found {
		t.Fatal("expected to find 'name'")
	}
	if string(entry.Value) != "John" {
		t.Errorf("expected 'John' got %q", entry.Value)
	}
	if entry.Deleted {
		t.Error("expected entry to not be deleted")
	}
}

// TestOverwrite verifies that setting a key twice updates the value.
func TestOverwrite(t *testing.T) {
	m := New()

	m.Set([]byte("key"), []byte("first"))
	m.Set([]byte("key"), []byte("second"))

	entry, found := m.Get([]byte("key"))
	if !found {
		t.Fatal("expected to find 'key'")
	}
	if string(entry.Value) != "second" {
		t.Errorf("expected 'second' got %q", entry.Value)
	}
}

// TestDelete verifies that deleting a key inserts a tombstone.
func TestDelete(t *testing.T) {
	m := New()

	m.Set([]byte("key"), []byte("value"))
	m.Delete([]byte("key"))

	entry, found := m.Get([]byte("key"))
	if !found {
		t.Fatal("expected to find tombstone for 'key'")
	}
	if !entry.Deleted {
		t.Error("expected entry to be marked deleted")
	}
	if entry.Value != nil {
		t.Errorf("expected nil value for tombstone, got %q", entry.Value)
	}
}

// TestDeleteNonExistentKey verifies that deleting a key that was
// never set still inserts a tombstone (needed for WAL replay ordering).
func TestDeleteNonExistentKey(t *testing.T) {
	m := New()

	m.Delete([]byte("ghost"))

	entry, found := m.Get([]byte("ghost"))
	if !found {
		t.Fatal("expected tombstone for non-existent key")
	}
	if !entry.Deleted {
		t.Error("expected tombstone")
	}
}

// TestGetMissing verifies that getting a key that was never set
// returns found=false.
func TestGetMissing(t *testing.T) {
	m := New()

	_, found := m.Get([]byte("missing"))
	if found {
		t.Error("expected not found for missing key")
	}
}

// TestIteratorOrder verifies that the iterator returns keys
// in sorted lexicographic order.
func TestIteratorOrder(t *testing.T) {
	m := New()

	// Insert in random order.
	keys := []string{"mango", "apple", "cherry", "banana", "date"}
	for _, k := range keys {
		m.Set([]byte(k), []byte("val:"+k))
	}

	// Expected sorted order.
	expected := []string{"apple", "banana", "cherry", "date", "mango"}

	it := m.NewIterator()
	defer it.Close()

	var got []string
	for ; it.Valid(); it.Next() {
		got = append(got, string(it.Key()))
	}

	if len(got) != len(expected) {
		t.Fatalf("expected %d keys got %d", len(expected), len(got))
	}
	for i, want := range expected {
		if got[i] != want {
			t.Errorf("position %d: want %q got %q", i, want, got[i])
		}
	}
}

// TestIteratorIncludesTombstones verifies that the iterator yields
// tombstone entries — the SSTable writer needs to see them.
func TestIteratorIncludesTombstones(t *testing.T) {
	m := New()

	m.Set([]byte("a"), []byte("alive"))
	m.Delete([]byte("b"))
	m.Set([]byte("c"), []byte("alive"))

	it := m.NewIterator()
	defer it.Close()

	var count int
	var deletedCount int
	for ; it.Valid(); it.Next() {
		count++
		if it.IsDeleted() {
			deletedCount++
		}
	}

	if count != 3 {
		t.Errorf("expected 3 entries got %d", count)
	}
	if deletedCount != 1 {
		t.Errorf("expected 1 tombstone got %d", deletedCount)
	}
}

// TestByteSize verifies that ByteSize grows with inserts.
func TestByteSize(t *testing.T) {
	m := New()

	if m.ByteSize() != 0 {
		t.Errorf("expected 0 bytes initially, got %d", m.ByteSize())
	}

	m.Set([]byte("key"), []byte("value")) // 3 + 5 = 8 bytes
	if m.ByteSize() != 8 {
		t.Errorf("expected 8 bytes got %d", m.ByteSize())
	}

	m.Set([]byte("key"), []byte("newvalue")) // value grows: 3 + 8 = 11
	if m.ByteSize() != 11 {
		t.Errorf("expected 11 bytes after overwrite got %d", m.ByteSize())
	}
}

// TestLen verifies that Len returns the correct entry count.
func TestLen(t *testing.T) {
	m := New()

	if m.Len() != 0 {
		t.Errorf("expected 0 length initially")
	}

	m.Set([]byte("a"), []byte("1"))
	m.Set([]byte("b"), []byte("2"))
	m.Delete([]byte("c"))

	if m.Len() != 3 {
		t.Errorf("expected 3 entries got %d", m.Len())
	}

	// Overwrite should NOT increase length.
	m.Set([]byte("a"), []byte("updated"))
	if m.Len() != 3 {
		t.Errorf("overwrite should not increase length, got %d", m.Len())
	}
}

// BenchmarkSet benchmarks sequential Set operations.
func BenchmarkSet(b *testing.B) {
	m := New()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key:%08d", i))
		m.Set(key, []byte("value"))
	}
}

// BenchmarkGet benchmarks random Get operations on a pre-filled MemTable.
func BenchmarkGet(b *testing.B) {
	m := New()
	for i := 0; i < 100_000; i++ {
		key := []byte(fmt.Sprintf("key:%08d", i))
		m.Set(key, []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key:%08d", i%100_000))
		m.Get(key)
	}
}

// BenchmarkIterator benchmarks a full sequential scan of the MemTable.
func BenchmarkIterator(b *testing.B) {
	m := New()
	for i := 0; i < 10_000; i++ {
		key := []byte(fmt.Sprintf("key:%08d", i))
		m.Set(key, []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		it := m.NewIterator()
		for ; it.Valid(); it.Next() {
			_ = it.Key()
			_ = it.Value()
		}
		it.Close()
	}
}
