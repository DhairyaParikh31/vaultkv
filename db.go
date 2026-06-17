package vaultkv

import (
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/DhairyaParikh31/vaultkv/internal/compaction"
	"github.com/DhairyaParikh31/vaultkv/internal/memtable"
	"github.com/DhairyaParikh31/vaultkv/internal/sstable"
	"github.com/DhairyaParikh31/vaultkv/internal/wal"
)

// DB is the main VaultKV database handle.
// It is safe for concurrent use by multiple goroutines.
type DB struct {
	opts Options

	walLog *wal.WAL

	mu         sync.RWMutex
	active     *memtable.MemTable
	immutable  *memtable.MemTable
	nextFileID uint64

	sstMu   sync.RWMutex
	readers []*sstable.Reader

	compact     *compaction.Engine
	flushChan   chan struct{}
	compactChan chan struct{}
	closeChan   chan struct{}
	wg          sync.WaitGroup
	closed      atomic.Bool
}

// Open opens or creates a VaultKV database in the given directory.
func Open(opts Options) (*DB, error) {
	opts = opts.withDefaults()
	if opts.Dir == "" {
		return nil, fmt.Errorf("vaultkv: Options.Dir is required")
	}
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, fmt.Errorf("vaultkv: create dir %q: %w", opts.Dir, err)
	}

	db := &DB{
		opts:        opts,
		active:      memtable.New(),
		compact:     compaction.NewEngine(opts.Dir, opts.SizeTieredThreshold),
		flushChan:   make(chan struct{}, 1),
		compactChan: make(chan struct{}, 1),
		closeChan:   make(chan struct{}),
		nextFileID:  1,
	}

	walOpts := wal.Options{SyncOnAppend: opts.SyncMode == SyncFull}
	w, err := wal.Open(opts.Dir, walOpts)
	if err != nil {
		return nil, fmt.Errorf("vaultkv: open WAL: %w", err)
	}
	db.walLog = w

	if err := db.loadSSTables(); err != nil {
		w.Close()
		return nil, fmt.Errorf("vaultkv: load SSTables: %w", err)
	}

	if err := db.replayWAL(); err != nil {
		w.Close()
		return nil, fmt.Errorf("vaultkv: replay WAL: %w", err)
	}

	db.wg.Add(2)
	go db.flushLoop()
	go db.compactionLoop()

	if opts.SyncMode == SyncBatched {
		db.wg.Add(1)
		go db.syncLoop()
	}

	return db, nil
}

// Set writes a key-value pair to the database.
func (db *DB) Set(key, value []byte) error {
	if db.closed.Load() {
		return fmt.Errorf("vaultkv: database is closed")
	}
	if len(key) == 0 {
		return fmt.Errorf("vaultkv: empty key not allowed")
	}
	if err := db.walLog.Append(wal.Record{Op: wal.OpSet, Key: key, Value: value}); err != nil {
		return fmt.Errorf("vaultkv: WAL append: %w", err)
	}
	db.mu.Lock()
	db.active.Set(key, value)
	shouldFlush := db.active.ByteSize() >= db.opts.MemTableSize
	db.mu.Unlock()

	if shouldFlush {
		select {
		case db.flushChan <- struct{}{}:
		default:
		}
	}
	return nil
}

// Delete removes a key from the database by writing a tombstone.
func (db *DB) Delete(key []byte) error {
	if db.closed.Load() {
		return fmt.Errorf("vaultkv: database is closed")
	}
	if len(key) == 0 {
		return fmt.Errorf("vaultkv: empty key not allowed")
	}
	if err := db.walLog.Append(wal.Record{Op: wal.OpDelete, Key: key}); err != nil {
		return fmt.Errorf("vaultkv: WAL append (delete): %w", err)
	}
	db.mu.Lock()
	db.active.Delete(key)
	db.mu.Unlock()
	return nil
}

// Get retrieves the value for a key.
// Returns nil, nil if the key is not found or has been deleted.
func (db *DB) Get(key []byte) ([]byte, error) {
	if db.closed.Load() {
		return nil, fmt.Errorf("vaultkv: database is closed")
	}

	// Step 1: active MemTable.
	db.mu.RLock()
	entry, found := db.active.Get(key)
	imm := db.immutable
	db.mu.RUnlock()

	if found {
		if entry.Deleted {
			return nil, nil
		}
		return entry.Value, nil
	}

	// Step 2: immutable MemTable.
	if imm != nil {
		entry, found = imm.Get(key)
		if found {
			if entry.Deleted {
				return nil, nil
			}
			return entry.Value, nil
		}
	}

	// Step 3: SSTable files, newest first.
	db.sstMu.RLock()
	readers := make([]*sstable.Reader, len(db.readers))
	copy(readers, db.readers)
	db.sstMu.RUnlock()

	for _, r := range readers {
		value, deleted, err := r.Get(key)
		if err != nil {
			return nil, fmt.Errorf("vaultkv: SSTable read: %w", err)
		}
		if deleted {
			return nil, nil
		}
		if value != nil {
			return value, nil
		}
	}

	return nil, nil
}

// Close flushes all pending writes and closes the database.
func (db *DB) Close() error {
	if !db.closed.CompareAndSwap(false, true) {
		return fmt.Errorf("vaultkv: already closed")
	}
	close(db.closeChan)
	select {
	case db.flushChan <- struct{}{}:
	default:
	}
	db.wg.Wait()

	db.sstMu.Lock()
	for _, r := range db.readers {
		r.Close()
	}
	db.readers = nil
	db.sstMu.Unlock()

	return db.walLog.Close()
}

// Stats returns a snapshot of current database metrics.
func (db *DB) Stats() Stats {
	db.mu.RLock()
	memBytes := db.active.ByteSize()
	memEntries := db.active.Len()
	db.mu.RUnlock()

	db.sstMu.RLock()
	sstCount := len(db.readers)
	db.sstMu.RUnlock()

	files := db.compact.Files()
	var totalDiskBytes int64
	for _, f := range files {
		totalDiskBytes += f.Size
	}

	return Stats{
		MemTableBytes:   memBytes,
		MemTableEntries: memEntries,
		SSTables:        sstCount,
		TotalDiskBytes:  totalDiskBytes,
	}
}

// Stats holds a snapshot of database metrics.
type Stats struct {
	MemTableBytes   int64
	MemTableEntries int
	SSTables        int
	TotalDiskBytes  int64
}

// ── Background goroutines ──────────────────────────────────────────────

func (db *DB) flushLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.closeChan:
			db.flushIfNeeded()
			return
		case <-db.flushChan:
			db.flushIfNeeded()
		}
	}
}

func (db *DB) flushIfNeeded() {
	db.mu.Lock()
	if db.active.Len() == 0 {
		db.mu.Unlock()
		return
	}
	if db.active.ByteSize() < db.opts.MemTableSize && !db.closed.Load() {
		db.mu.Unlock()
		return
	}
	db.immutable = db.active
	db.active = memtable.New()
	fileID := db.nextSSTableID()
	db.mu.Unlock()

	if err := db.flushMemTable(db.immutable, fileID); err != nil {
		fmt.Printf("vaultkv: flush error: %v\n", err)
	}

	db.mu.Lock()
	db.immutable = nil
	db.mu.Unlock()

	select {
	case db.compactChan <- struct{}{}:
	default:
	}
}

func (db *DB) flushMemTable(mem *memtable.MemTable, fileID uint64) error {
	if err := sstable.WriteFromMemTable(db.opts.Dir, fileID, mem, db.opts.BlockSize); err != nil {
		return fmt.Errorf("write SSTable: %w", err)
	}
	path := filepath.Join(db.opts.Dir, fmt.Sprintf("%016x.sst", fileID))
	info, err := os.Stat(path)
	if err != nil {
		return fmt.Errorf("stat SSTable: %w", err)
	}
	r, err := sstable.Open(path)
	if err != nil {
		return fmt.Errorf("open SSTable reader: %w", err)
	}
	db.sstMu.Lock()
	db.readers = append([]*sstable.Reader{r}, db.readers...)
	db.sstMu.Unlock()

	db.compact.AddFile(&compaction.FileMetadata{
		FileID: fileID,
		Path:   path,
		Size:   info.Size(),
		Level:  0,
	})
	return nil
}

func (db *DB) compactionLoop() {
	defer db.wg.Done()
	for {
		select {
		case <-db.closeChan:
			return
		case <-db.compactChan:
			if db.compact.NeedsCompaction() {
				outputID := db.nextSSTableID()
				if _, err := db.compact.RunOnce(outputID); err != nil {
					fmt.Printf("vaultkv: compaction error: %v\n", err)
				}
				db.reloadReaders()
			}
		}
	}
}

func (db *DB) syncLoop() {
	defer db.wg.Done()
	interval := time.Duration(db.opts.SyncIntervalMs) * time.Millisecond
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-db.closeChan:
			return
		case <-ticker.C:
			if err := db.walLog.Sync(); err != nil {
				fmt.Printf("vaultkv: WAL sync error: %v\n", err)
			}
		}
	}
}

// ── Startup helpers ────────────────────────────────────────────────────

func (db *DB) loadSSTables() error {
	pattern := filepath.Join(db.opts.Dir, "*.sst")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return fmt.Errorf("glob SSTables: %w", err)
	}
	sort.Strings(paths)

	for _, path := range paths {
		r, err := sstable.Open(path)
		if err != nil {
			fmt.Printf("vaultkv: skip corrupt SSTable %q: %v\n", path, err)
			continue
		}
		db.readers = append([]*sstable.Reader{r}, db.readers...)
		info, _ := os.Stat(path)
		var fileID uint64
		fmt.Sscanf(filepath.Base(path), "%016x.sst", &fileID)
		db.compact.AddFile(&compaction.FileMetadata{
			FileID: fileID,
			Path:   path,
			Size:   info.Size(),
			Level:  0,
		})
		if fileID >= db.nextFileID {
			db.nextFileID = fileID + 1
		}
	}
	return nil
}

func (db *DB) replayWAL() error {
	var count int
	err := db.walLog.Replay(func(rec wal.Record) error {
		switch rec.Op {
		case wal.OpSet:
			db.active.Set(rec.Key, rec.Value)
		case wal.OpDelete:
			db.active.Delete(rec.Key)
		}
		count++
		return nil
	})
	if err != nil {
		return fmt.Errorf("WAL replay: %w", err)
	}
	if count > 0 {
		fmt.Printf("vaultkv: replayed %d records from WAL\n", count)
	}
	return nil
}

func (db *DB) reloadReaders() {
	pattern := filepath.Join(db.opts.Dir, "*.sst")
	paths, err := filepath.Glob(pattern)
	if err != nil {
		return
	}
	sort.Strings(paths)

	newReaders := make([]*sstable.Reader, 0, len(paths))
	for i := len(paths) - 1; i >= 0; i-- {
		r, err := sstable.Open(paths[i])
		if err != nil {
			continue
		}
		newReaders = append(newReaders, r)
	}

	db.sstMu.Lock()
	old := db.readers
	db.readers = newReaders
	db.sstMu.Unlock()

	for _, r := range old {
		r.Close()
	}
}

func (db *DB) nextSSTableID() uint64 {
	return atomic.AddUint64(&db.nextFileID, 1)
}
