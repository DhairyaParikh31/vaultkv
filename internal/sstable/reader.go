package sstable

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"

	"github.com/DhairyaParikh31/vaultkv/internal/bloom"
)

// Reader reads from a single SSTable file.
// On Open, the footer and index block are loaded into memory.
// The Bloom filter is also loaded into memory.
// Data blocks are read from disk on demand.
type Reader struct {
	file       *os.File
	fileSize   int64
	index      []indexEntry  // sparse index — one entry per data block
	filter     *bloom.Filter // Bloom filter for this SSTable
	numEntries uint64
}

// Open opens an SSTable file for reading.
// Loads the footer, index block, and Bloom filter into memory.
// Returns an error if the magic number does not match (corrupt file).
func Open(path string) (*Reader, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("sstable reader: open %q: %w", path, err)
	}

	info, err := f.Stat()
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable reader: stat: %w", err)
	}

	fileSize := info.Size()
	if fileSize < footerSize {
		f.Close()
		return nil, fmt.Errorf("sstable reader: file too small (%d bytes)", fileSize)
	}

	r := &Reader{file: f, fileSize: fileSize}

	// ── Read and parse footer ──────────────────────────────────────────
	footerBuf := make([]byte, footerSize)
	if _, err := f.ReadAt(footerBuf, fileSize-footerSize); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable reader: read footer: %w", err)
	}

	indexOffset := binary.LittleEndian.Uint64(footerBuf[0:8])
	indexLen := binary.LittleEndian.Uint32(footerBuf[8:12])
	bloomOffset := binary.LittleEndian.Uint64(footerBuf[12:20])
	bloomLen := binary.LittleEndian.Uint32(footerBuf[20:24])
	r.numEntries = binary.LittleEndian.Uint64(footerBuf[24:32])
	fileMagic := binary.LittleEndian.Uint64(footerBuf[32:40])
	fileVersion := binary.LittleEndian.Uint16(footerBuf[40:42])

	// Validate magic number.
	if fileMagic != magic {
		f.Close()
		return nil, fmt.Errorf("sstable reader: invalid magic number %016x (expected %016x)", fileMagic, magic)
	}

	// Validate version.
	if fileVersion != version {
		f.Close()
		return nil, fmt.Errorf("sstable reader: unsupported version %d", fileVersion)
	}

	// ── Read and parse index block ─────────────────────────────────────
	indexBuf := make([]byte, indexLen)
	if _, err := f.ReadAt(indexBuf, int64(indexOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable reader: read index: %w", err)
	}

	r.index, err = parseIndex(indexBuf)
	if err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable reader: parse index: %w", err)
	}

	// ── Read and decode Bloom filter ───────────────────────────────────
	bloomBuf := make([]byte, bloomLen)
	if _, err := f.ReadAt(bloomBuf, int64(bloomOffset)); err != nil {
		f.Close()
		return nil, fmt.Errorf("sstable reader: read bloom filter: %w", err)
	}

	r.filter = bloom.Decode(bloomBuf)

	return r, nil
}

// Get looks up a key in the SSTable.
//
// Returns:
//
//	value, false, nil  → key found, not deleted
//	nil,   true,  nil  → key found but tombstoned
//	nil,   false, nil  → key not in this SSTable
//	nil,   false, err  → I/O or corruption error
//
// Read path:
//  1. Query Bloom filter — return "not found" immediately if definitely absent
//  2. Binary search the sparse index to find the candidate data block
//  3. Read the data block from disk and scan for the key
func (r *Reader) Get(key []byte) (value []byte, deleted bool, err error) {
	// Step 1 — Bloom filter check.
	// If the filter says DEFINITELY ABSENT, skip the disk read entirely.
	if !r.filter.MayContain(key) {
		return nil, false, nil
	}

	// Step 2 — Binary search the sparse index.
	// Find the rightmost index entry whose firstKey <= key.
	blockIdx := r.searchIndex(key)
	if blockIdx < 0 {
		return nil, false, nil
	}

	// Step 3 — Read the data block and scan for the key.
	ie := r.index[blockIdx]
	blockData, err := r.readBlock(ie.blockOffset, ie.blockLen)
	if err != nil {
		return nil, false, err
	}

	return scanBlock(blockData, key)
}

// searchIndex performs a binary search over the sparse index to find
// the index of the data block that might contain the given key.
//
// Returns the index of the rightmost entry whose firstKey <= key,
// or -1 if key is smaller than all firstKeys (not in file).
func (r *Reader) searchIndex(key []byte) int {
	lo, hi := 0, len(r.index)-1
	result := -1

	for lo <= hi {
		mid := (lo + hi) / 2
		cmp := bytes.Compare(r.index[mid].firstKey, key)
		if cmp <= 0 {
			result = mid
			lo = mid + 1
		} else {
			hi = mid - 1
		}
	}

	return result
}

// readBlock reads a data block from disk at the given offset and length.
// Verifies the CRC32 trailer before returning the block data.
func (r *Reader) readBlock(offset uint64, length uint32) ([]byte, error) {
	buf := make([]byte, length)
	if _, err := r.file.ReadAt(buf, int64(offset)); err != nil {
		return nil, fmt.Errorf("sstable reader: read block at offset %d: %w", offset, err)
	}

	if len(buf) < 4 {
		return nil, fmt.Errorf("sstable reader: block too small (%d bytes)", len(buf))
	}

	// Verify CRC32 trailer (last 4 bytes of the block).
	storedCRC := binary.LittleEndian.Uint32(buf[len(buf)-4:])
	data := buf[:len(buf)-4]
	computedCRC := crc32.Checksum(data, crcTable)

	if storedCRC != computedCRC {
		return nil, fmt.Errorf("sstable reader: block CRC mismatch (stored=%08x computed=%08x)", storedCRC, computedCRC)
	}

	return data, nil
}

// scanBlock performs a linear scan of a data block looking for key.
// Entries within a block are sorted, so we can stop early.
//
// Returns value, isDeleted, error — same semantics as Get.
func scanBlock(data, key []byte) ([]byte, bool, error) {
	offset := 0

	for offset < len(data) {
		if offset+entryHeaderSize > len(data) {
			return nil, false, fmt.Errorf("sstable reader: truncated entry header at offset %d", offset)
		}

		keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		valLen := binary.LittleEndian.Uint32(data[offset+4 : offset+8])
		flags := data[offset+8]
		offset += entryHeaderSize

		if offset+int(keyLen)+int(valLen) > len(data) {
			return nil, false, fmt.Errorf("sstable reader: entry payload exceeds block at offset %d", offset)
		}

		entryKey := data[offset : offset+int(keyLen)]
		offset += int(keyLen)

		cmp := bytes.Compare(entryKey, key)

		if cmp == 0 {
			// Found the key.
			if flags == flagTombstone {
				return nil, true, nil
			}
			value := make([]byte, valLen)
			copy(value, data[offset:offset+int(valLen)])
			return value, false, nil
		}

		if cmp > 0 {
			// Entries are sorted — key cannot appear later in the block.
			return nil, false, nil
		}

		offset += int(valLen)
	}

	return nil, false, nil
}

// NewIterator returns an iterator that reads all entries in sorted order.
// The iterator reads data blocks sequentially from the SSTable file.
func (r *Reader) NewIterator() *SSTableIterator {
	return &SSTableIterator{
		reader:     r,
		blockIndex: 0,
		blockData:  nil,
		offset:     0,
		valid:      true,
	}
}

// NumEntries returns the number of entries in the SSTable.
func (r *Reader) NumEntries() uint64 {
	return r.numEntries
}

// Close closes the underlying SSTable file.
func (r *Reader) Close() error {
	return r.file.Close()
}

// parseIndex deserializes the index block into a slice of indexEntry.
func parseIndex(data []byte) ([]indexEntry, error) {
	var entries []indexEntry
	offset := 0

	for offset < len(data) {
		if offset+4 > len(data) {
			return nil, fmt.Errorf("truncated index entry at offset %d", offset)
		}

		keyLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		if offset+int(keyLen)+12 > len(data) {
			return nil, fmt.Errorf("index entry payload exceeds buffer at offset %d", offset)
		}

		firstKey := make([]byte, keyLen)
		copy(firstKey, data[offset:offset+int(keyLen)])
		offset += int(keyLen)

		blockOffset := binary.LittleEndian.Uint64(data[offset : offset+8])
		offset += 8

		blockLen := binary.LittleEndian.Uint32(data[offset : offset+4])
		offset += 4

		entries = append(entries, indexEntry{
			firstKey:    firstKey,
			blockOffset: blockOffset,
			blockLen:    blockLen,
		})
	}

	return entries, nil
}

// ── SSTableIterator ────────────────────────────────────────────────────

// SSTableIterator iterates over all entries in an SSTable in sorted order.
type SSTableIterator struct {
	reader     *Reader
	blockIndex int    // current block index in the sparse index
	blockData  []byte // current block data (nil = not loaded yet)
	offset     int    // current byte offset within blockData
	valid      bool

	// current entry
	currKey     []byte
	currValue   []byte
	currDeleted bool
}

// Valid returns true if the iterator points to a valid entry.
func (it *SSTableIterator) Valid() bool {
	return it.valid
}

// Next advances to the next entry.
func (it *SSTableIterator) Next() {
	it.advance()
}

// Key returns the current entry's key.
func (it *SSTableIterator) Key() []byte {
	return it.currKey
}

// Value returns the current entry's value.
// Returns nil for tombstone entries.
func (it *SSTableIterator) Value() []byte {
	return it.currValue
}

// IsDeleted returns true if the current entry is a tombstone.
func (it *SSTableIterator) IsDeleted() bool {
	return it.currDeleted
}

// advance loads the next entry, fetching the next block if needed.
func (it *SSTableIterator) advance() {
	for {
		// If no block is loaded, load the current block.
		if it.blockData == nil {
			if it.blockIndex >= len(it.reader.index) {
				it.valid = false
				return
			}
			ie := it.reader.index[it.blockIndex]
			data, err := it.reader.readBlock(ie.blockOffset, ie.blockLen)
			if err != nil {
				it.valid = false
				return
			}
			it.blockData = data
			it.offset = 0
		}

		// Try to read the next entry from the current block.
		if it.offset >= len(it.blockData) {
			// Block exhausted — move to next block.
			it.blockIndex++
			it.blockData = nil
			it.offset = 0
			continue
		}

		if it.offset+entryHeaderSize > len(it.blockData) {
			it.valid = false
			return
		}

		keyLen := binary.LittleEndian.Uint32(it.blockData[it.offset : it.offset+4])
		valLen := binary.LittleEndian.Uint32(it.blockData[it.offset+4 : it.offset+8])
		flags := it.blockData[it.offset+8]
		it.offset += entryHeaderSize

		if it.offset+int(keyLen)+int(valLen) > len(it.blockData) {
			it.valid = false
			return
		}

		it.currKey = make([]byte, keyLen)
		copy(it.currKey, it.blockData[it.offset:it.offset+int(keyLen)])
		it.offset += int(keyLen)

		it.currDeleted = flags == flagTombstone
		if !it.currDeleted && valLen > 0 {
			it.currValue = make([]byte, valLen)
			copy(it.currValue, it.blockData[it.offset:it.offset+int(valLen)])
		} else {
			it.currValue = nil
		}
		it.offset += int(valLen)

		it.valid = true
		return
	}
}

// SeekToFirst positions the iterator at the first entry.
func (it *SSTableIterator) SeekToFirst() {
	it.blockIndex = 0
	it.blockData = nil
	it.offset = 0
	it.valid = true
	it.advance()
}

// Implements io.Closer for cleanup.
func (it *SSTableIterator) Close() error {
	it.valid = false
	it.blockData = nil
	return nil
}

// Ensure SSTableIterator satisfies io.Closer.
var _ io.Closer = (*SSTableIterator)(nil)
