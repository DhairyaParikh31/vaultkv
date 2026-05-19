package memtable

import (
	"fmt"
	"testing"
)

// TestSetAndGet verifies basic set and get operations.
func TestSetAndGet(t *testing.T) {
	m := New()

	m.Set([]byte("name"), []byte("alice"))
	m.Set([]byte("city"), []byte("atlanta"))

	entry, found := m.Get([]byte("name"))
	if !found {
		t.Fatal("expected to find 'name'")
	}
	if string(entry.Value) != "alice" {
		t.Errorf("expected 'alice' got %q", entry.Value)
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