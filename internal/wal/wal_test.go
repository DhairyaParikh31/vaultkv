package wal

import (
	"os"
	"path/filepath"
	"testing"
)

// TestAppendAndReplay verifies that records written via Append
// are correctly recovered by Replay.
func TestAppendAndReplay(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	records := []Record{
		{Op: OpSet, Key: []byte("name"), Value: []byte("John")},
		{Op: OpSet, Key: []byte("age"), Value: []byte("30")},
		{Op: OpDelete, Key: []byte("name")},
		{Op: OpSet, Key: []byte("city"), Value: []byte("vadodara")},
	}

	for _, rec := range records {
		if err := w.Append(rec); err != nil {
			t.Fatalf("Append: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	// Reopen and replay.
	w2, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatalf("Open (replay): %v", err)
	}
	defer w2.Close()

	var replayed []Record
	if err := w2.Replay(func(r Record) error {
		replayed = append(replayed, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay: %v", err)
	}

	if len(replayed) != len(records) {
		t.Fatalf("expected %d records, got %d", len(records), len(replayed))
	}

	for i, want := range records {
		got := replayed[i]
		if got.Op != want.Op {
			t.Errorf("record[%d]: op want %v got %v", i, want.Op, got.Op)
		}
		if string(got.Key) != string(want.Key) {
			t.Errorf("record[%d]: key want %q got %q", i, want.Key, got.Key)
		}
		if string(got.Value) != string(want.Value) {
			t.Errorf("record[%d]: value want %q got %q", i, want.Value, got.Value)
		}
	}
}

// TestDeleteRecord verifies that OpDelete records have no value.
func TestDeleteRecord(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	if err := w.Append(Record{Op: OpDelete, Key: []byte("gone")}); err != nil {
		t.Fatalf("Append delete: %v", err)
	}
	w.Close()

	w2, _ := Open(dir, Options{})
	defer w2.Close()

	var got []Record
	w2.Replay(func(r Record) error {
		got = append(got, r)
		return nil
	})

	if len(got) != 1 {
		t.Fatalf("expected 1 record got %d", len(got))
	}
	if got[0].Op != OpDelete {
		t.Errorf("expected OpDelete got %v", got[0].Op)
	}
	if len(got[0].Value) != 0 {
		t.Errorf("delete record should have empty value, got %q", got[0].Value)
	}
}

// TestCorruptedRecordTruncation verifies that a corrupt WAL record
// causes replay to stop and the WAL to be truncated at that point.
func TestCorruptedRecordTruncation(t *testing.T) {
	dir := t.TempDir()
	w, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatalf("Open: %v", err)
	}

	// Write two valid records.
	_ = w.Append(Record{Op: OpSet, Key: []byte("k1"), Value: []byte("v1")})
	_ = w.Append(Record{Op: OpSet, Key: []byte("k2"), Value: []byte("v2")})
	w.Close()

	// Corrupt the WAL by flipping bytes in the middle of the file.
	path := filepath.Join(dir, walFileName)
	data, _ := os.ReadFile(path)

	// Flip bytes starting at recordHeaderSize + 10 (inside second record).
	corruptAt := recordHeaderSize + 10
	if corruptAt < len(data) {
		data[corruptAt] ^= 0xFF
	}
	os.WriteFile(path, data, 0644)

	// Replay — should recover first record only and truncate.
	w2, err := Open(dir, Options{SyncOnAppend: true})
	if err != nil {
		t.Fatalf("Open after corrupt: %v", err)
	}
	defer w2.Close()

	var replayed []Record
	if err := w2.Replay(func(r Record) error {
		replayed = append(replayed, r)
		return nil
	}); err != nil {
		t.Fatalf("Replay after corrupt: %v", err)
	}

	// Only first record should survive.
	if len(replayed) != 1 {
		t.Fatalf("expected 1 record after corruption, got %d", len(replayed))
	}
	if string(replayed[0].Key) != "k1" {
		t.Errorf("expected k1, got %q", replayed[0].Key)
	}
}

// TestPartialWriteTruncation simulates a crash mid-write by truncating
// the WAL file in the middle of a record.
func TestPartialWriteTruncation(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Options{SyncOnAppend: true})
	_ = w.Append(Record{Op: OpSet, Key: []byte("complete"), Value: []byte("yes")})
	w.Close()

	// Manually append partial bytes simulating a crash mid-write.
	path := filepath.Join(dir, walFileName)
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0644)
	// Write a partial header — only 6 bytes instead of 13+.
	f.Write([]byte{0x00, 0x00, 0x00, 0x00, 0x53, 0x05})
	f.Close()

	w2, _ := Open(dir, Options{SyncOnAppend: true})
	defer w2.Close()

	var replayed []Record
	w2.Replay(func(r Record) error {
		replayed = append(replayed, r)
		return nil
	})

	// Only the complete record should be replayed.
	if len(replayed) != 1 {
		t.Fatalf("expected 1 complete record, got %d", len(replayed))
	}
	if string(replayed[0].Key) != "complete" {
		t.Errorf("expected 'complete', got %q", replayed[0].Key)
	}
}

// TestEmptyWALReplay verifies that replaying an empty WAL is a no-op.
func TestEmptyWALReplay(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Options{})
	defer w.Close()

	var count int
	if err := w.Replay(func(r Record) error {
		count++
		return nil
	}); err != nil {
		t.Fatalf("Replay empty WAL: %v", err)
	}
	if count != 0 {
		t.Errorf("expected 0 records from empty WAL, got %d", count)
	}
}

// TestLargeKeyValue verifies that large keys and values are
// encoded and decoded correctly.
func TestLargeKeyValue(t *testing.T) {
	dir := t.TempDir()
	w, _ := Open(dir, Options{SyncOnAppend: true})

	key := make([]byte, 1024)
	value := make([]byte, 64*1024) // 64KB value
	for i := range key {
		key[i] = byte(i % 256)
	}
	for i := range value {
		value[i] = byte(i % 256)
	}

	if err := w.Append(Record{Op: OpSet, Key: key, Value: value}); err != nil {
		t.Fatalf("Append large record: %v", err)
	}
	w.Close()

	w2, _ := Open(dir, Options{})
	defer w2.Close()

	var replayed []Record
	w2.Replay(func(r Record) error {
		replayed = append(replayed, r)
		return nil
	})

	if len(replayed) != 1 {
		t.Fatalf("expected 1 record, got %d", len(replayed))
	}
	if string(replayed[0].Key) != string(key) {
		t.Error("large key mismatch")
	}
	if string(replayed[0].Value) != string(value) {
		t.Error("large value mismatch")
	}
}

// BenchmarkAppendSync benchmarks WAL appends with full fsync per write.
// This is the baseline for the "durability cost" benchmark in the README.
func BenchmarkAppendSync(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir, Options{SyncOnAppend: true})
	defer w.Close()

	rec := Record{Op: OpSet, Key: []byte("benchkey"), Value: []byte("benchvalue")}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(rec); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

// BenchmarkAppendNoSync benchmarks WAL appends without fsync.
// Represents the maximum throughput ceiling of the WAL.
func BenchmarkAppendNoSync(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir, Options{SyncOnAppend: false})
	defer w.Close()

	rec := Record{Op: OpSet, Key: []byte("benchkey"), Value: []byte("benchvalue")}
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := w.Append(rec); err != nil {
			b.Fatalf("Append: %v", err)
		}
	}
}

// BenchmarkReplay benchmarks replaying a WAL with 100,000 records.
func BenchmarkReplay(b *testing.B) {
	dir := b.TempDir()
	w, _ := Open(dir, Options{SyncOnAppend: false})

	rec := Record{Op: OpSet, Key: []byte("benchkey"), Value: make([]byte, 64)}
	for i := 0; i < 100_000; i++ {
		w.Append(rec)
	}
	w.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		w2, _ := Open(dir, Options{})
		w2.Replay(func(r Record) error { return nil })
		w2.Close()
	}
}