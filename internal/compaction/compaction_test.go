package compaction

import (
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/DhairyaParikh31/vaultkv/internal/memtable"
	"github.com/DhairyaParikh31/vaultkv/internal/sstable"
)

func buildSSTable(t *testing.T, e *Engine, fileID uint64, entries map[string]string, deleted []string) *FileMetadata {
	t.Helper()
	mem := memtable.New()
	for k, v := range entries {
		mem.Set([]byte(k), []byte(v))
	}
	for _, k := range deleted {
		mem.Delete([]byte(k))
	}
	if err := sstable.WriteFromMemTable(e.dir, fileID, mem, 0); err != nil {
		t.Fatalf("WriteFromMemTable: %v", err)
	}
	path := filepath.Join(e.dir, fmt.Sprintf("%016x.sst", fileID))
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat SSTable: %v", err)
	}
	meta := &FileMetadata{FileID: fileID, Path: path, Size: info.Size(), Level: 0}
	e.AddFile(meta)
	return meta
}

func TestNeedsCompactionFalse(t *testing.T) {
	e := NewEngine(t.TempDir(), 4)
	for i := uint64(1); i <= 3; i++ {
		buildSSTable(t, e, i, map[string]string{fmt.Sprintf("key%d", i): fmt.Sprintf("val%d", i)}, nil)
	}
	if e.NeedsCompaction() {
		t.Error("expected NeedsCompaction=false with 3 files (threshold=4)")
	}
}

func TestNeedsCompactionTrue(t *testing.T) {
	e := NewEngine(t.TempDir(), 4)
	for i := uint64(1); i <= 4; i++ {
		buildSSTable(t, e, i, map[string]string{
			fmt.Sprintf("key%d-a", i): "value",
			fmt.Sprintf("key%d-b", i): "value",
		}, nil)
	}
	if !e.NeedsCompaction() {
		t.Error("expected NeedsCompaction=true with 4 files (threshold=4)")
	}
}

func TestRunOnceReducesFileCount(t *testing.T) {
	e := NewEngine(t.TempDir(), 4)
	for i := uint64(1); i <= 4; i++ {
		buildSSTable(t, e, i, map[string]string{fmt.Sprintf("animal%d", i): fmt.Sprintf("species%d", i)}, nil)
	}
	before := len(e.Files())
	compacted, err := e.RunOnce(5)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	after := len(e.Files())
	if compacted != 4 {
		t.Errorf("expected 4 files compacted, got %d", compacted)
	}
	if after >= before {
		t.Errorf("expected fewer files after compaction: before=%d after=%d", before, after)
	}
	if after != 1 {
		t.Errorf("expected 1 file after compacting 4 into 1, got %d", after)
	}
}

func TestMergePreservesAllKeys(t *testing.T) {
	dir := t.TempDir()
	e := NewEngine(dir, 4)
	allExpected := map[string]string{}
	for i := uint64(1); i <= 4; i++ {
		entries := map[string]string{
			fmt.Sprintf("key-%d-alpha", i): fmt.Sprintf("val-%d-alpha", i),
			fmt.Sprintf("key-%d-beta", i):  fmt.Sprintf("val-%d-beta", i),
		}
		for k, v := range entries {
			allExpected[k] = v
		}
		buildSSTable(t, e, i, entries, nil)
	}
	_, err := e.RunOnce(5)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	files := e.Files()
	if len(files) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(files))
	}
	r, err := sstable.Open(files[0].Path)
	if err != nil {
		t.Fatalf("Open merged SSTable: %v", err)
	}
	defer r.Close()
	for key, wantVal := range allExpected {
		val, deleted, err := r.Get([]byte(key))
		if err != nil {
			t.Fatalf("Get(%q): %v", key, err)
		}
		if deleted {
			t.Errorf("Get(%q): unexpected tombstone", key)
		}
		if string(val) != wantVal {
			t.Errorf("Get(%q): want %q got %q", key, wantVal, val)
		}
	}
}

func TestMergeDeduplicatesKeys(t *testing.T) {
	dir := t.TempDir()
	e := NewEngine(dir, 4)
	buildSSTable(t, e, 1, map[string]string{"shared-key": "old-value", "unique-1": "v1"}, nil)
	buildSSTable(t, e, 2, map[string]string{"shared-key": "middle-value", "unique-2": "v2"}, nil)
	buildSSTable(t, e, 3, map[string]string{"shared-key": "newest-value", "unique-3": "v3"}, nil)
	buildSSTable(t, e, 4, map[string]string{"unique-4": "v4"}, nil)
	_, err := e.RunOnce(5)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	files := e.Files()
	if len(files) != 1 {
		t.Fatalf("expected 1 merged file, got %d", len(files))
	}
	r, err := sstable.Open(files[0].Path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	defer r.Close()
	val, _, err := r.Get([]byte("shared-key"))
	if err != nil {
		t.Fatalf("Get(shared-key): %v", err)
	}
	if string(val) != "newest-value" {
		t.Errorf("dedup: expected 'newest-value' got %q", val)
	}
}

func TestGroupBySize(t *testing.T) {
	files := []*FileMetadata{
		{FileID: 1, Size: 100},
		{FileID: 2, Size: 110},
		{FileID: 3, Size: 90},
		{FileID: 4, Size: 5000},
		{FileID: 5, Size: 4800},
	}
	tiers := groupBySize(files)
	if len(tiers) != 2 {
		t.Errorf("expected 2 tiers, got %d", len(tiers))
	}
	found3, found2 := false, false
	for _, tier := range tiers {
		if len(tier) == 3 {
			found3 = true
		}
		if len(tier) == 2 {
			found2 = true
		}
	}
	if !found3 || !found2 {
		t.Errorf("expected tiers of size 3 and 2")
	}
}

func TestRunOnceNoCompactionNeeded(t *testing.T) {
	e := NewEngine(t.TempDir(), 4)
	for i := uint64(1); i <= 2; i++ {
		buildSSTable(t, e, i, map[string]string{fmt.Sprintf("k%d", i): "v"}, nil)
	}
	compacted, err := e.RunOnce(5)
	if err != nil {
		t.Fatalf("RunOnce: %v", err)
	}
	if compacted != 0 {
		t.Errorf("expected 0 files compacted, got %d", compacted)
	}
}

func BenchmarkRunOnce(b *testing.B) {
	for i := 0; i < b.N; i++ {
		b.StopTimer()
		dir := b.TempDir()
		e := NewEngine(dir, 4)
		for id := uint64(1); id <= 4; id++ {
			mem := memtable.New()
			for j := 0; j < 1000; j++ {
				mem.Set([]byte(fmt.Sprintf("key-%d-%08d", id, j)), []byte("value"))
			}
			sstable.WriteFromMemTable(dir, id, mem, 0)
			path := filepath.Join(dir, fmt.Sprintf("%016x.sst", id))
			info, _ := os.Stat(path)
			e.AddFile(&FileMetadata{FileID: id, Path: path, Size: info.Size()})
		}
		b.StartTimer()
		e.RunOnce(5)
	}
}
