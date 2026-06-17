// Package sstable implements the on-disk file format for VaultKV.
//
// An SSTable (Sorted String Table) is an immutable, sorted file written
// once when a MemTable is flushed to disk. Once written, it is never
// modified — only read or replaced during compaction.
//
// SSTable File Layout:
//
//	┌─────────────────────────────────────────────────────────┐
//	│                    Data Blocks                          │
//	│  Block 0: [Entry 0][Entry 1]...[Entry N][Trailer]       │
//	│  Block 1: [Entry N+1]...[Trailer]                       │
//	│  ...                                                    │
//	├─────────────────────────────────────────────────────────┤
//	│                    Index Block                          │
//	│  [IndexEntry 0][IndexEntry 1]...[IndexEntry M]          │
//	│  One entry per data block (sparse index)                │
//	├─────────────────────────────────────────────────────────┤
//	│                  Bloom Filter Block                     │
//	│  [k uint8][m uint32][bit_array bytes]                   │
//	├─────────────────────────────────────────────────────────┤
//	│                  Footer (48 bytes)                      │
//	│  [index_offset uint64][index_len uint32]                │
//	│  [bloom_offset uint64][bloom_len uint32]                │
//	│  [num_entries  uint64]                                  │
//	│  [magic        uint64] = 0x5661756C744B5600             │
//	│  [version      uint16]                                  │
//	│  [reserved     6 bytes]                                 │
//	└─────────────────────────────────────────────────────────┘
//
// Data Block Entry Format:
//
//	[key_len uint32][val_len uint32][flags uint8][key bytes][val bytes]
//	flags: 0x00 = normal, 0x01 = tombstone (deleted key)
//
// Index Entry Format:
//
//	[key_len uint32][key bytes][block_offset uint64][block_len uint32]
//
// Footer is always the last 48 bytes of the file.
// The magic number "VaultKV\0" identifies a valid SSTable.
package sstable

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"os"
	"path/filepath"

	"github.com/DhairyaParikh31/vaultkv/internal/bloom"
	"github.com/DhairyaParikh31/vaultkv/internal/memtable"
)

const (
	// Magic number identifies a valid VaultKV SSTable file.
	// ASCII bytes: "VaultKV\0"
	magic uint64 = 0x5661756C744B5600

	// version is the current SSTable format version.
	version uint16 = 1

	// footerSize is the fixed size of the SSTable footer in bytes.
	// 8+4+8+4+8+8+2+6 = 48 bytes
	footerSize = 48

	// defaultBlockSize is the target size for data blocks in bytes.
	defaultBlockSize = 4096

	// entryHeaderSize is the fixed overhead per data block entry:
	// 4 (key_len) + 4 (val_len) + 1 (flags) = 9 bytes
	entryHeaderSize = 9

	// indexEntryHeaderSize is fixed overhead per index entry:
	// 4 (key_len) + 8 (block_offset) + 4 (block_len) = 16 bytes
	indexEntryHeaderSize = 16

	// flagNormal marks a regular key-value entry.
	flagNormal byte = 0x00

	// flagTombstone marks a deleted key entry.
	flagTombstone byte = 0x01

	// bitsPerKey is the Bloom filter bits per key at default FPP (~1%).
	bitsPerKey = 9.6
)

// crcTable is the CRC-32/IEEE polynomial table.
var crcTable = crc32.MakeTable(crc32.IEEE)

// indexEntry records the first key and file offset of a data block.
// The sparse index holds one indexEntry per data block.
type indexEntry struct {
	firstKey    []byte
	blockOffset uint64
	blockLen    uint32
}

// Writer writes a single SSTable file from a sorted sequence of entries.
// Usage:
//
//	w, err := NewWriter(dir, fileID, blockSize)
//	for each entry (in sorted key order):
//	    err = w.Add(key, value, isDeleted)
//	err = w.Finish()
type Writer struct {
	file      *os.File
	buf       *bufio.Writer
	offset    uint64 // current write position in file

	// current data block being assembled
	blockBuf    []byte // entries accumulated for the current block
	blockStart  uint64 // file offset where current block begins
	blockSize   int    // target block size in bytes

	// index: one entry per flushed data block
	index []indexEntry

	// all keys seen — used to build the Bloom filter at Finish()
	allKeys [][]byte

	numEntries uint64
	firstKey   []byte // first key of the current block (for index)
}

// NewWriter creates a new SSTable writer that writes to dir/fileID.sstable.
// blockSize controls the target data block size (0 = defaultBlockSize).
func NewWriter(dir string, fileID uint64, blockSize int) (*Writer, error) {
	if blockSize <= 0 {
		blockSize = defaultBlockSize
	}

	path := filepath.Join(dir, fmt.Sprintf("%016x.sst", fileID))
	f, err := os.Create(path)
	if err != nil {
		return nil, fmt.Errorf("sstable writer: create %q: %w", path, err)
	}

	return &Writer{
		file:      f,
		buf:       bufio.NewWriterSize(f, 64*1024), // 64KB write buffer
		blockSize: blockSize,
	}, nil
}

// WriteFromMemTable flushes an entire MemTable to the SSTable.
// Entries are written in sorted key order (guaranteed by MemTable iterator).
// This is the primary way SSTables are created — from MemTable flushes.
func WriteFromMemTable(dir string, fileID uint64, mem *memtable.MemTable, blockSize int) error {
	w, err := NewWriter(dir, fileID, blockSize)
	if err != nil {
		return err
	}

	it := mem.NewIterator()
	for ; it.Valid(); it.Next() {
		if err := w.Add(it.Key(), it.Value(), it.IsDeleted()); err != nil {
			it.Close()
			return err
		}
	}
	it.Close()

	return w.Finish()
}

// Add appends a key-value entry to the SSTable.
// Keys MUST be added in strictly increasing lexicographic order.
// isDeleted=true writes a tombstone entry (value is ignored).
func (w *Writer) Add(key, value []byte, isDeleted bool) error {
	if len(key) == 0 {
		return fmt.Errorf("sstable writer: empty key not allowed")
	}

	// Record the first key of the current block for the sparse index.
	if len(w.blockBuf) == 0 {
		w.firstKey = make([]byte, len(key))
		copy(w.firstKey, key)
		w.blockStart = w.offset
	}

	// Build the entry: [key_len uint32][val_len uint32][flags byte][key][val]
	valLen := uint32(len(value))
	if isDeleted {
		valLen = 0
		value = nil
	}

	flags := flagNormal
	if isDeleted {
		flags = flagTombstone
	}

	entrySize := entryHeaderSize + len(key) + int(valLen)
	entry := make([]byte, entrySize)
	binary.LittleEndian.PutUint32(entry[0:4], uint32(len(key)))
	binary.LittleEndian.PutUint32(entry[4:8], valLen)
	entry[8] = flags
	copy(entry[9:], key)
	if !isDeleted {
		copy(entry[9+len(key):], value)
	}

	w.blockBuf = append(w.blockBuf, entry...)
	w.allKeys = append(w.allKeys, append([]byte(nil), key...))
	w.numEntries++

	// Flush the current block if it has reached the target size.
	if len(w.blockBuf) >= w.blockSize {
		if err := w.flushBlock(); err != nil {
			return err
		}
	}

	return nil
}

// flushBlock writes the current accumulated block buffer to disk,
// appends a CRC32 trailer, and records an index entry.
func (w *Writer) flushBlock() error {
	if len(w.blockBuf) == 0 {
		return nil
	}

	// Compute CRC32 over the block data.
	checksum := crc32.Checksum(w.blockBuf, crcTable)

	// Write block data.
	n, err := w.buf.Write(w.blockBuf)
	if err != nil {
		return fmt.Errorf("sstable writer: write block data: %w", err)
	}
	w.offset += uint64(n)

	// Write 4-byte CRC32 trailer.
	var trailer [4]byte
	binary.LittleEndian.PutUint32(trailer[:], checksum)
	if _, err := w.buf.Write(trailer[:]); err != nil {
		return fmt.Errorf("sstable writer: write block trailer: %w", err)
	}
	w.offset += 4

	blockLen := uint32(len(w.blockBuf) + 4) // data + trailer

	// Record index entry for this block.
	w.index = append(w.index, indexEntry{
		firstKey:    w.firstKey,
		blockOffset: w.blockStart,
		blockLen:    blockLen,
	})

	// Reset block buffer.
	w.blockBuf = w.blockBuf[:0]
	w.firstKey = nil

	return nil
}

// Finish flushes any remaining data, writes the index block,
// Bloom filter block, and footer, then closes the file.
// Must be called exactly once after all Add() calls.
func (w *Writer) Finish() error {
	// Flush any remaining entries in the current block.
	if err := w.flushBlock(); err != nil {
		return err
	}

	// ── Write Index Block ──────────────────────────────────────────────
	indexOffset := w.offset

	for _, ie := range w.index {
		// [key_len uint32][key bytes][block_offset uint64][block_len uint32]
		keyLen := uint32(len(ie.firstKey))
		entry := make([]byte, indexEntryHeaderSize+int(keyLen))

		binary.LittleEndian.PutUint32(entry[0:4], keyLen)
		copy(entry[4:4+keyLen], ie.firstKey)
		binary.LittleEndian.PutUint64(entry[4+keyLen:12+keyLen], ie.blockOffset)
		binary.LittleEndian.PutUint32(entry[12+keyLen:16+keyLen], ie.blockLen)

		n, err := w.buf.Write(entry)
		if err != nil {
			return fmt.Errorf("sstable writer: write index entry: %w", err)
		}
		w.offset += uint64(n)
	}

	indexLen := uint32(w.offset - indexOffset)

	// ── Write Bloom Filter Block ───────────────────────────────────────
	bloomOffset := w.offset

	f := bloom.Build(w.allKeys, bitsPerKey)
	encoded := f.Encode()

	n, err := w.buf.Write(encoded)
	if err != nil {
		return fmt.Errorf("sstable writer: write bloom filter: %w", err)
	}
	w.offset += uint64(n)

	bloomLen := uint32(w.offset - bloomOffset)

	// ── Write Footer (48 bytes) ────────────────────────────────────────
	// [index_offset uint64][index_len uint32]   = 12 bytes
	// [bloom_offset uint64][bloom_len uint32]   = 12 bytes
	// [num_entries  uint64]                     =  8 bytes
	// [magic        uint64]                     =  8 bytes
	// [version      uint16]                     =  2 bytes
	// [reserved     6 bytes]                    =  6 bytes
	// Total                                     = 48 bytes

	footer := make([]byte, footerSize)
	binary.LittleEndian.PutUint64(footer[0:8], indexOffset)
	binary.LittleEndian.PutUint32(footer[8:12], indexLen)
	binary.LittleEndian.PutUint64(footer[12:20], bloomOffset)
	binary.LittleEndian.PutUint32(footer[20:24], bloomLen)
	binary.LittleEndian.PutUint64(footer[24:32], w.numEntries)
	binary.LittleEndian.PutUint64(footer[32:40], magic)
	binary.LittleEndian.PutUint16(footer[40:42], version)
	// footer[42:48] is reserved — zero-padded by make()

	if _, err := w.buf.Write(footer); err != nil {
		return fmt.Errorf("sstable writer: write footer: %w", err)
	}

	// Flush the buffered writer to the OS.
	if err := w.buf.Flush(); err != nil {
		return fmt.Errorf("sstable writer: flush: %w", err)
	}

	// fsync — guarantee the SSTable is durably on disk.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("sstable writer: fsync: %w", err)
	}

	return w.file.Close()
}
