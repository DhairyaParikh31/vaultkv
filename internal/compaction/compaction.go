// Package compaction implements the background SSTable merging engine
// for VaultKV.
//
// Without compaction, SSTable files accumulate unboundedly. Each
// additional file increases read amplification (more files to check
// per lookup) and wastes space with superseded values and tombstones.
//
// Compaction merges multiple SSTables into fewer, larger files:
//   - Eliminates duplicate keys (keeps only the newest value)
//   - Removes tombstones when no older version exists
//   - Reduces read amplification by consolidating files
//
// VaultKV implements Size-Tiered Compaction (default):
//
//	Groups SSTables of similar size into tiers.
//	Triggers when a tier accumulates >= threshold files.
//	Optimized for write-heavy workloads.
//	Lower write amplification, higher space amplification.
//
// The Amplification Triad (no strategy minimizes all three):
//
//	Write Amplification (WA) = bytes_written / user_bytes_written
//	Read Amplification  (RA) = disk_reads per user read (worst case)
//	Space Amplification (SA) = disk_bytes / live_data_bytes
//
//	Size-Tiered: WA=1.4x  RA=8x   SA=2.1x  (write-optimized)
//	Level-Based: WA=3.2x  RA=1.8x SA=1.1x  (read-optimized)
package compaction

import (
	"bytes"
	"container/heap"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"github.com/DhairyaParikh31/vaultkv/internal/sstable"
)

// FileMetadata describes a single SSTable file managed by the engine.
type FileMetadata struct {
	FileID  uint64
	Path    string
	Size    int64
	Level   int
	MinKey  []byte
	MaxKey  []byte
	Entries uint64
}

// Engine manages the set of SSTable files and runs compaction.
type Engine struct {
	mu        sync.RWMutex
	dir       string
	files     []*FileMetadata
	threshold int
	running   bool
}

// NewEngine creates a compaction engine for the given directory.
func NewEngine(dir string, threshold int) *Engine {
	if threshold <= 0 {
		threshold = 4
	}
	return &Engine{
		dir:       dir,
		threshold: threshold,
	}
}

// AddFile registers a newly flushed SSTable with the engine.
func (e *Engine) AddFile(meta *FileMetadata) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.files = append(e.files, meta)
}

// Files returns a snapshot of the current SSTable file list.
func (e *Engine) Files() []*FileMetadata {
	e.mu.RLock()
	defer e.mu.RUnlock()
	result := make([]*FileMetadata, len(e.files))
	copy(result, e.files)
	return result
}

// NeedsCompaction returns true if any size tier has accumulated
// enough files to trigger compaction.
func (e *Engine) NeedsCompaction() bool {
	e.mu.RLock()
	defer e.mu.RUnlock()
	tiers := groupBySize(e.files)
	for _, tier := range tiers {
		if len(tier) >= e.threshold {
			return true
		}
	}
	return false
}

// RunOnce performs one round of size-tiered compaction if needed.
func (e *Engine) RunOnce(outputID uint64) (int, error) {
	e.mu.Lock()

	if e.running {
		e.mu.Unlock()
		return 0, nil
	}

	tiers := groupBySize(e.files)
	var selected []*FileMetadata
	for _, tier := range tiers {
		if len(tier) >= e.threshold {
			if len(tier) > len(selected) {
				selected = tier
			}
		}
	}

	if len(selected) == 0 {
		e.mu.Unlock()
		return 0, nil
	}

	e.running = true
	e.mu.Unlock()

	outputPath, outputMeta, err := e.merge(selected, outputID)
	if err != nil {
		e.mu.Lock()
		e.running = false
		e.mu.Unlock()
		return 0, fmt.Errorf("compaction: merge: %w", err)
	}

	e.mu.Lock()
	defer e.mu.Unlock()

	selectedIDs := make(map[uint64]bool)
	for _, f := range selected {
		selectedIDs[f.FileID] = true
	}

	remaining := e.files[:0]
	for _, f := range e.files {
		if !selectedIDs[f.FileID] {
			remaining = append(remaining, f)
		}
	}

	remaining = append(remaining, outputMeta)
	e.files = remaining
	e.running = false

	for _, f := range selected {
		if err := os.Remove(f.Path); err != nil && !os.IsNotExist(err) {
			fmt.Printf("compaction: warning: remove %q: %v\n", f.Path, err)
		}
	}

	_ = outputPath
	return len(selected), nil
}

// merge performs an N-way merge of selected SSTable files.
func (e *Engine) merge(files []*FileMetadata, outputID uint64) (string, *FileMetadata, error) {
	sort.Slice(files, func(i, j int) bool {
		return files[i].FileID < files[j].FileID
	})

	readers := make([]*sstable.Reader, 0, len(files))
	iterators := make([]*sstable.SSTableIterator, 0, len(files))

	for _, f := range files {
		r, err := sstable.Open(f.Path)
		if err != nil {
			for _, r2 := range readers {
				r2.Close()
			}
			return "", nil, fmt.Errorf("open %q: %w", f.Path, err)
		}
		readers = append(readers, r)
		it := r.NewIterator()
		it.SeekToFirst()
		iterators = append(iterators, it)
	}

	defer func() {
		for i, r := range readers {
			iterators[i].Close()
			r.Close()
		}
	}()

	h := &mergeHeap{}
	for i, it := range iterators {
		if it.Valid() {
			heap.Push(h, &heapItem{
				key:      it.Key(),
				value:    it.Value(),
				deleted:  it.IsDeleted(),
				fileIdx:  i,
				fileID:   files[i].FileID,
				iterator: it,
			})
		}
	}
	heap.Init(h)

	w, err := sstable.NewWriter(e.dir, outputID, 0)
	if err != nil {
		return "", nil, fmt.Errorf("create output SSTable: %w", err)
	}

	var (
		lastKey    []byte
		numEntries uint64
		minKey     []byte
		maxKey     []byte
	)

	for h.Len() > 0 {
		item := heap.Pop(h).(*heapItem)

		item.iterator.Next()
		if item.iterator.Valid() {
			heap.Push(h, &heapItem{
				key:      item.iterator.Key(),
				value:    item.iterator.Value(),
				deleted:  item.iterator.IsDeleted(),
				fileIdx:  item.fileIdx,
				fileID:   files[item.fileIdx].FileID,
				iterator: item.iterator,
			})
		}

		if lastKey != nil && bytes.Equal(item.key, lastKey) {
			continue
		}
		lastKey = item.key

		if item.deleted && len(files) == len(e.files) {
			continue
		}

		if err := w.Add(item.key, item.value, item.deleted); err != nil {
			return "", nil, fmt.Errorf("write merged entry: %w", err)
		}

		if minKey == nil {
			minKey = append([]byte(nil), item.key...)
		}
		maxKey = append([]byte(nil), item.key...)
		numEntries++
	}

	if err := w.Finish(); err != nil {
		return "", nil, fmt.Errorf("finish output SSTable: %w", err)
	}

	outputPath := filepath.Join(e.dir, fmt.Sprintf("%016x.sst", outputID))
	info, err := os.Stat(outputPath)
	if err != nil {
		return "", nil, fmt.Errorf("stat output SSTable: %w", err)
	}

	meta := &FileMetadata{
		FileID:  outputID,
		Path:    outputPath,
		Size:    info.Size(),
		Level:   0,
		MinKey:  minKey,
		MaxKey:  maxKey,
		Entries: numEntries,
	}

	return outputPath, meta, nil
}

// ── Size-Tiered Grouping ───────────────────────────────────────────────

const sizeTierRatio = 2.0

func groupBySize(files []*FileMetadata) [][]*FileMetadata {
	if len(files) == 0 {
		return nil
	}

	sorted := make([]*FileMetadata, len(files))
	copy(sorted, files)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].Size < sorted[j].Size
	})

	var tiers [][]*FileMetadata
	var current []*FileMetadata

	for _, f := range sorted {
		if len(current) == 0 {
			current = append(current, f)
			continue
		}
		smallest := current[0].Size
		if smallest == 0 {
			smallest = 1
		}
		ratio := float64(f.Size) / float64(smallest)
		if ratio < sizeTierRatio {
			current = append(current, f)
		} else {
			tiers = append(tiers, current)
			current = []*FileMetadata{f}
		}
	}

	if len(current) > 0 {
		tiers = append(tiers, current)
	}

	return tiers
}

// ── Merge Heap ─────────────────────────────────────────────────────────

type heapItem struct {
	key      []byte
	value    []byte
	deleted  bool
	fileIdx  int
	fileID   uint64
	iterator *sstable.SSTableIterator
}

type mergeHeap []*heapItem

func (h mergeHeap) Len() int { return len(h) }

func (h mergeHeap) Less(i, j int) bool {
	cmp := bytes.Compare(h[i].key, h[j].key)
	if cmp != 0 {
		return cmp < 0
	}
	return h[i].fileID > h[j].fileID
}

func (h mergeHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h *mergeHeap) Push(x interface{}) {
	*h = append(*h, x.(*heapItem))
}

func (h *mergeHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}
