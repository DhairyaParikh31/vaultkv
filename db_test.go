package vaultkv

import (
	"fmt"
	"os"
	"testing"
	"time"
)

// TestOpenClose verifies that a database can be opened and closed cleanly.
func TestOpenClose(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// TestSetAndGet verifies basic write and read operations.
func TestSetAndGet(t *testing.T) {
	dir := t.TempDir()
	db, err := Open(Options{Dir: dir})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer db.Close()

	if err := db.Set([]byte("name"), []byte("vaultkv")); err != nil {
		t.Fatalf("Set: %v", err)
	}

	val, err := db.Get([]byte("name"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "vaultkv" {
		t.Errorf("expected 'vaultkv' got %q", val)
	}
}

// TestGetMissing verifies that a missing key returns nil, nil.
func TestGetMissing(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	defer db.Close()

	val, err := db.Get([]byte("missing"))
	if err != nil {
		t.Fatalf("Get missing: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil for missing key, got %q", val)
	}
}

// TestDelete verifies that deleting a key makes it invisible.
func TestDelete(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	defer db.Close()

	db.Set([]byte("key"), []byte("value"))
	db.Delete([]byte("key"))

	val, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get after delete: %v", err)
	}
	if val != nil {
		t.Errorf("expected nil after delete, got %q", val)
	}
}

// TestOverwrite verifies that the latest value for a key is returned.
func TestOverwrite(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	defer db.Close()

	db.Set([]byte("key"), []byte("first"))
	db.Set([]byte("key"), []byte("second"))
	db.Set([]byte("key"), []byte("third"))

	val, err := db.Get([]byte("key"))
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if string(val) != "third" {
		t.Errorf("expected 'third' got %q", val)
	}
}

// TestEmptyKeyRejected verifies that empty keys return an error.
func TestEmptyKeyRejected(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	defer db.Close()

	if err := db.Set([]byte{}, []byte("value")); err == nil {
		t.Error("expected error for empty key on Set")
	}
	if err := db.Delete([]byte{}); err == nil {
		t.Error("expected error for empty key on Delete")
	}
}

// TestCrashRecovery verifies that committed writes survive a simulated
// crash — close without flush, reopen, and verify data is recovered.
func TestCrashRecovery(t *testing.T) {
	dir := t.TempDir()

	// Write some data and close cleanly (WAL is fsynced on close).
	db, err := Open(Options{Dir: dir, SyncMode: SyncFull})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	db.Set([]byte("survived"), []byte("yes"))
	db.Set([]byte("also-survived"), []byte("yes"))
	db.Close()

	// Reopen — WAL should be replayed.
	db2, err := Open(Options{Dir: dir, SyncMode: SyncFull})
	if err != nil {
		t.Fatalf("Open after crash: %v", err)
	}
	defer db2.Close()

	val, err := db2.Get([]byte("survived"))
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(val) != "yes" {
		t.Errorf("crash recovery: expected 'yes' got %q", val)
	}

	val, err = db2.Get([]byte("also-survived"))
	if err != nil {
		t.Fatalf("Get after recovery: %v", err)
	}
	if string(val) != "yes" {
		t.Errorf("crash recovery: expected 'yes' got %q", val)
	}
}

// TestPersistenceAcrossReopen verifies that data written and flushed
// to SSTable is readable after a clean reopen.
func TestPersistenceAcrossReopen(t *testing.T) {
	dir := t.TempDir()

	// Write enough data to trigger a MemTable flush.
	db, err := Open(Options{
		Dir:          dir,
		MemTableSize: 1024, // tiny — forces flush quickly
		SyncMode:     SyncFull,
	})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		val := []byte(fmt.Sprintf("val-%03d", i))
		db.Set(key, val)
	}

	// Give the flush goroutine time to write SSTables.
	time.Sleep(100 * time.Millisecond)
	db.Close()

	// Reopen and verify data.
	db2, err := Open(Options{Dir: dir, SyncMode: SyncFull})
	if err != nil {
		t.Fatalf("Open after flush: %v", err)
	}
	defer db2.Close()

	for i := 0; i < 100; i++ {
		key := []byte(fmt.Sprintf("key-%03d", i))
		wantVal := fmt.Sprintf("val-%03d", i)
		val, err := db2.Get(key)
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if string(val) != wantVal {
			t.Errorf("Get(%q): want %q got %q", key, wantVal, val)
		}
	}
}

// TestStats verifies that Stats returns reasonable values.
func TestStats(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	defer db.Close()

	stats := db.Stats()
	if stats.MemTableBytes != 0 {
		t.Errorf("expected 0 MemTableBytes initially, got %d", stats.MemTableBytes)
	}

	db.Set([]byte("key"), []byte("value"))
	stats = db.Stats()
	if stats.MemTableBytes == 0 {
		t.Error("expected non-zero MemTableBytes after Set")
	}
	if stats.MemTableEntries != 1 {
		t.Errorf("expected 1 MemTableEntry, got %d", stats.MemTableEntries)
	}
}

// TestUseAfterClose verifies that operations after Close return errors.
func TestUseAfterClose(t *testing.T) {
	dir := t.TempDir()
	db, _ := Open(Options{Dir: dir})
	db.Close()

	if err := db.Set([]byte("key"), []byte("val")); err == nil {
		t.Error("expected error on Set after Close")
	}
	if _, err := db.Get([]byte("key")); err == nil {
		t.Error("expected error on Get after Close")
	}
	if err := db.Delete([]byte("key")); err == nil {
		t.Error("expected error on Delete after Close")
	}
}

// BenchmarkSet benchmarks sequential write throughput with full fsync.
func BenchmarkSetSync(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(Options{Dir: dir, SyncMode: SyncFull})
	defer db.Close()

	val := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key:%08d", i))
		db.Set(key, val)
	}
}

// BenchmarkSetNoSync benchmarks write throughput without fsync.
func BenchmarkSetNoSync(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(Options{Dir: dir, SyncMode: SyncNone})
	defer db.Close()

	val := make([]byte, 64)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		key := []byte(fmt.Sprintf("key:%08d", i))
		db.Set(key, val)
	}
}

// BenchmarkGet benchmarks random read throughput from MemTable.
func BenchmarkGet(b *testing.B) {
	dir := b.TempDir()
	db, _ := Open(Options{Dir: dir, SyncMode: SyncNone})
	defer db.Close()

	n := 100_000
	for i := 0; i < n; i++ {
		db.Set([]byte(fmt.Sprintf("key:%08d", i)), []byte("value"))
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		db.Get([]byte(fmt.Sprintf("key:%08d", i%n)))
	}
}

// BenchmarkSetNaive is the naive baseline for comparison:
// a simple file write per operation (no WAL, no LSM).
// This is what VaultKV is benchmarked against in the README.
func BenchmarkSetNaive(b *testing.B) {
	dir := b.TempDir()
	path := dir + "/naive.db"
	val := []byte("benchmarkvalue64byteslong_______________________end")

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Naive: open, write, close per operation (worst case baseline).
		f, _ := os.OpenFile(path, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0644)
		f.Write([]byte(fmt.Sprintf("key:%08d=", i)))
		f.Write(val)
		f.Write([]byte("\n"))
		f.Sync()
		f.Close()
	}
}