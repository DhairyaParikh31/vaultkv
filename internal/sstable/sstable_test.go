package sstable

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DhairyaParikh31/vaultkv/internal/memtable"
)

// buildTestSSTable creates an SSTable with the given entries and
// returns the path to the file.
func buildTestSSTable(t *testing.T, entries []struct {
	key     string
	value   string
	deleted bool
}, blockSize int) string {
	t.Helper()
	dir := t.TempDir()

	w, err := NewWriter(dir, 1, blockSize)
	if err != nil {
		t.Fatalf("NewWriter: %v", err)
	}

	for _, e := range entries {
		if err := w.Add([]byte(e.key), []byte(e.value), e.deleted); err != nil {
			t.Fatalf("Add: %v", err)
		}
	}

	if err := w.Finish(); err != nil {
		t.Fatalf("Finish: %v", err)
	}

	return filepath.Join(dir, fmt.Sprintf("%016x.sst", 1))
}

// TestWriteAndReadBasic verifies that entries written to an SSTable
// can be read back correctly.
func TestWriteAndReadBasic(t *testing.T) {
	entries := []struct {
		key     string
		value   string
		deleted bool
	}{
		{"apple", "fruit:apple", false},
		{"banana", "fruit:banana", false},
		{"cherry", "fruit:cherry", false},
	}

	path := buildTestSSTable(t, entries, defaultBlockSize)

	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	for _, e := range entries {
		val, deleted, err := r.Get([]byte(e.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", e.key, err)
		}
		if deleted {
			t.Errorf("Get(%q): unexpected tombstone", e.key)
		}
		if string(val) != e.value {
			t.Errorf("Get(%q): want %q got %q", e.key, e.value, val)
		}
	}
}

// TestTombstoneEntry verifies that deleted entries are returned
// as tombstones with deleted=true and nil value.
func TestTombstoneEntry(t *testing.T) {
	entries := []struct {
		key     string
		value   string
		deleted bool
	}{
		{"alive", "still here", false},
		{"dead", "", true},
	}

	path := buildTestSSTable(t, entries, defaultBlockSize)
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Normal entry.
	val, deleted, err := r.Get([]byte("alive"))
	if err != nil || deleted || string(val) != "still here" {
		t.Errorf("alive: val=%q deleted=%v err=%v", val, deleted, err)
	}

	// Tombstone entry.
	val, deleted, err = r.Get([]byte("dead"))
	if err != nil {
		t.Fatalf("Get(dead): %v", err)
	}
	if !deleted {
		t.Error("expected tombstone for 'dead'")
	}
	if val != nil {
		t.Errorf("tombstone should have nil value, got %q", val)
	}
}

// TestAbsentKey verifies that looking up a key not in the SSTable
// returns found=false with no error.
func TestAbsentKey(t *testing.T) {
	entries := []struct {
		key     string
		value   string
		deleted bool
	}{
		{"aardvark", "mammal", false},
		{"zebra", "mammal", false},
	}

	path := buildTestSSTable(t, entries, defaultBlockSize)
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	val, deleted, err := r.Get([]byte("elephant"))
	if err != nil {
		t.Fatalf("Get(absent): %v", err)
	}
	if deleted || val != nil {
		t.Errorf("absent key should return nil, false, nil — got val=%q deleted=%v", val, deleted)
	}
}

// TestMagicNumberValidation verifies that opening a corrupt SSTable
// (wrong magic number) returns an error.
func TestMagicNumberValidation(t *testing.T) {
	entries := []struct {
		key     string
		value   string
		deleted bool
	}{
		{"key", "value", false},
	}

	path := buildTestSSTable(t, entries, defaultBlockSize)

	// Corrupt the last 8 bytes of the footer (magic number).
	data, _ := os.ReadFile(path)
	for i := len(data) - 16; i < len(data)-8; i++ {
		data[i] ^= 0xFF
	}
	os.WriteFile(path, data, 0644)

	_, err := Open(path)
	if err == nil {
		t.Error("expected error for corrupt magic number, got nil")
	}
}

// TestMultipleBlocks verifies correct read behavior when entries
// span multiple data blocks (small block size forces multiple blocks).
func TestMultipleBlocks(t *testing.T) {
	// Use a tiny block size (64 bytes) to force multiple blocks.
	smallBlock := 64
	var entries []struct {
		key     string
		value   string
		deleted bool
	}

	for i := 0; i < 50; i++ {
		entries = append(entries, struct {
			key     string
			value   string
			deleted bool
		}{
			key:     fmt.Sprintf("key-%03d", i),
			value:   fmt.Sprintf("value-%03d", i),
			deleted: false,
		})
	}

	path := buildTestSSTable(t, entries, smallBlock)
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// Verify all entries are retrievable.
	for _, e := range entries {
		val, deleted, err := r.Get([]byte(e.key))
		if err != nil {
			t.Fatalf("Get(%q): %v", e.key, err)
		}
		if deleted {
			t.Errorf("Get(%q): unexpected tombstone", e.key)
		}
		if string(val) != e.value {
			t.Errorf("Get(%q): want %q got %q", e.key, e.value, val)
		}
	}
}

// TestIteratorOrder verifies that the SSTable iterator yields
// entries in sorted key order.
func TestIteratorOrder(t *testing.T) {
	entries := []struct {
		key     string
		value   string
		deleted bool
	}{
		{"apple", "a", false},
		{"banana", "b", false},
		{"cherry", "c", true}, // tombstone
		{"date", "d", false},
		{"elderberry", "e", false},
	}

	path := buildTestSSTable(t, entries, defaultBlockSize)
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	it := r.NewIterator()
	it.SeekToFirst()

	var keys []string
	for ; it.Valid(); it.Next() {
		keys = append(keys, string(it.Key()))
	}
	it.Close()

	expected := []string{"apple", "banana", "cherry", "date", "elderberry"}
	if len(keys) != len(expected) {
		t.Fatalf("expected %d keys got %d", len(expected), len(keys))
	}
	for i, want := range expected {
		if keys[i] != want {
			t.Errorf("position %d: want %q got %q", i, want, keys[i])
		}
	}
}

// TestWriteFromMemTable verifies the WriteFromMemTable convenience function.
func TestWriteFromMemTable(t *testing.T) {
	dir := t.TempDir()
	mem := memtable.New()

	mem.Set([]byte("city"), []byte("vadodara"))
	mem.Set([]byte("name"), []byte("vaultkv"))
	mem.Delete([]byte("old"))
	mem.Set([]byte("version"), []byte("1.0"))

	if err := WriteFromMemTable(dir, 1, mem, defaultBlockSize); err != nil {
		t.Fatalf("WriteFromMemTable: %v", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("%016x.sst", 1))
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	// city should be found.
	val, deleted, err := r.Get([]byte("city"))
	if err != nil || deleted || string(val) != "vadodara" {
		t.Errorf("city: val=%q deleted=%v err=%v", val, deleted, err)
	}

	// old should be a tombstone.
	_, deleted, err = r.Get([]byte("old"))
	if err != nil || !deleted {
		t.Errorf("old: expected tombstone, deleted=%v err=%v", deleted, err)
	}

	// missing should not be found.
	val, deleted, err = r.Get([]byte("missing"))
	if err != nil || deleted || val != nil {
		t.Errorf("missing: expected not found, val=%q deleted=%v err=%v", val, deleted, err)
	}
}

// TestNumEntries verifies that NumEntries returns the correct count.
func TestNumEntries(t *testing.T) {
	entries := []struct {
		key     string
		value   string
		deleted bool
	}{
		{"a", "1", false},
		{"b", "2", false},
		{"c", "", true},
		{"d", "4", false},
	}

	path := buildTestSSTable(t, entries, defaultBlockSize)
	r, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()

	if r.NumEntries() != uint64(len(entries)) {
		t.Errorf("NumEntries: want %d got %d", len(entries), r.NumEntries())
	}
}

// BenchmarkGet benchmarks random point lookups in a 10K-entry SSTable.
func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	mem := memtable.New()

	n := 10_000
	for i := 0; i < n; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i))
		val := []byte(fmt.Sprintf("val-%08d", i))
		mem.Set(key, val)
	}

	if err := WriteFromMemTable(dir, 1, mem, defaultBlockSize); err != nil {
		b.Fatalf("WriteFromMemTable: %v", err)
	}

	path := filepath.Join(dir, fmt.Sprintf("%016x.sst", 1))
	r, err := Open(path)
	if err != nil {
		b.Fatalf("Open: %v", err)
	}
	defer r.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key-%08d", i%n))
		r.Get(key)
	}
}

// BenchmarkGetAbsent benchmarks lookups for keys not in the SSTable.
// This exercises the Bloom filter's ability to avoid disk reads.
func BenchmarkGetAbsent(b *testing.B) {
	dir := b.TempDir()
	mem := memtable.New()

	for i := 0; i < 10_000; i++ {
		mem.Set([]byte(fmt.Sprintf("key-%08d", i)), []byte("val"))
	}

	WriteFromMemTable(dir, 1, mem, defaultBlockSize)
	path := filepath.Join(dir, fmt.Sprintf("%016x.sst", 1))
	r, _ := Open(path)
	defer r.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// These keys are NOT in the SSTable.
		r.Get([]byte(fmt.Sprintf("absent-%08d", i)))
	}
}
