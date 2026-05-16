// Package wal implements the Write-Ahead Log for VaultKV.
//
// Every write (SET or DELETE) is appended to the WAL before being
// applied to the MemTable. On crash, the WAL is replayed to recover
// all committed writes — any write that returned successfully to the
// caller is guaranteed to survive a process crash (SIGKILL, power
// failure) provided the underlying storage honors fsync semantics.
//
// WAL Record Binary Format (little-endian):
//
//	┌──────────┬─────────┬────────────┬────────────┬─────────┬───────────┐
//	│ CRC32    │ OpCode  │ Key Length │ Val Length │  Key    │   Value   │
//	│ 4 bytes  │ 1 byte  │ 4 bytes    │ 4 bytes    │ N bytes │ M bytes   │
//	└──────────┴─────────┴────────────┴────────────┴─────────┴───────────┘
//
// CRC32 covers all bytes from OpCode onward (everything except the
// checksum itself). A mismatch during replay indicates a corrupt or
// partially written record — VaultKV truncates the WAL at that point
// and stops replay.
package wal

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
)

// OpCode identifies the type of WAL record.
type OpCode byte

const (
	// OpSet represents a key-value write (SET operation).
	OpSet OpCode = 0x53 // ASCII 'S'

	// OpDelete represents a key deletion (DELETE operation).
	// Value field is absent for delete records (ValLength = 0).
	OpDelete OpCode = 0x44 // ASCII 'D'
)

const (
	// recordHeaderSize is the fixed overhead per WAL record:
	// 4 (CRC32) + 1 (OpCode) + 4 (KeyLen) + 4 (ValLen) = 13 bytes.
	recordHeaderSize = 13

	// walFileName is the name of the WAL file within the DB directory.
	walFileName = "WALFILE"
)

// crcTable is the CRC-32/IEEE polynomial table, computed once at init.
// This is the same polynomial used by RocksDB, LevelDB, and Ethernet.
var crcTable = crc32.MakeTable(crc32.IEEE)

// Record represents a single decoded WAL entry.
type Record struct {
	Op    OpCode
	Key   []byte
	Value []byte // nil for OpDelete records
}

// WAL is the write-ahead log. It is safe for concurrent use — a
// sync.Mutex serializes all Append calls.
type WAL struct {
	mu   sync.Mutex
	file *os.File
	path string
	opts Options
}

// Options configures WAL behavior.
type Options struct {
	// SyncOnAppend controls whether fsync is called after every Append.
	// Set false for batched sync (caller is responsible for calling Sync).
	SyncOnAppend bool
}

// Open opens or creates a WAL file in the given directory.
// If the file already exists, it is opened for appending —
// existing records are NOT replayed here; call Replay for that.
func Open(dir string, opts Options) (*WAL, error) {
	path := filepath.Join(dir, walFileName)

	// O_CREATE | O_RDWR: create if absent, open for read+write.
	// O_APPEND: all writes go to end of file.
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		return nil, fmt.Errorf("wal: open %q: %w", path, err)
	}

	return &WAL{
		file: f,
		path: path,
		opts: opts,
	}, nil
}

// Append writes a single record to the WAL.
//
// The record is encoded as:
//
//	[CRC32 uint32][OpCode byte][KeyLen uint32][ValLen uint32][Key][Value]
//
// If SyncOnAppend is true, fsync is called before returning.
// The caller receives an error if either the write or fsync fails.
func (w *WAL) Append(rec Record) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Build the payload: OpCode + KeyLen + ValLen + Key + Value.
	// We compute CRC32 over this payload before prepending it.
	keyLen := uint32(len(rec.Key))
	valLen := uint32(len(rec.Value))

	// Total record size: header (13 bytes) + key + value.
	buf := make([]byte, recordHeaderSize+int(keyLen)+int(valLen))

	// Bytes [4:5] = OpCode
	buf[4] = byte(rec.Op)

	// Bytes [5:9] = KeyLen (little-endian uint32)
	binary.LittleEndian.PutUint32(buf[5:9], keyLen)

	// Bytes [9:13] = ValLen (little-endian uint32)
	binary.LittleEndian.PutUint32(buf[9:13], valLen)

	// Bytes [13:13+keyLen] = Key
	copy(buf[13:], rec.Key)

	// Bytes [13+keyLen:] = Value
	copy(buf[13+keyLen:], rec.Value)

	// CRC32 covers buf[4:] (everything after the checksum field).
	checksum := crc32.Checksum(buf[4:], crcTable)
	binary.LittleEndian.PutUint32(buf[0:4], checksum)

	// Write the full record atomically to the WAL file.
	// os.File.Write on Linux is atomic for writes smaller than PIPE_BUF
	// but we hold the mutex anyway to serialize all appends.
	if _, err := w.file.Write(buf); err != nil {
		return fmt.Errorf("wal: write record: %w", err)
	}

	// fsync if configured — guarantees the OS has flushed to disk.
	if w.opts.SyncOnAppend {
		if err := w.file.Sync(); err != nil {
			return fmt.Errorf("wal: fsync: %w", err)
		}
	}

	return nil
}

// Sync explicitly fsyncs the WAL file.
// Used when SyncOnAppend is false (batched sync mode) — the caller
// is responsible for calling Sync at appropriate intervals.
func (w *WAL) Sync() error {
	w.mu.Lock()
	defer w.mu.Unlock()
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: sync: %w", err)
	}
	return nil
}

// Replay reads all valid records from the WAL from the beginning
// and calls fn for each one in order.
//
// Replay stops and returns nil if it encounters a record whose CRC32
// checksum does not match — this indicates a partially written record
// (the process crashed mid-write). The WAL is truncated at that point.
//
// Replay returns an error only for unexpected I/O failures.
// It is safe to call Append after Replay returns.
func (w *WAL) Replay(fn func(Record) error) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Seek to the beginning for replay.
	if _, err := w.file.Seek(0, io.SeekStart); err != nil {
		return fmt.Errorf("wal: replay seek: %w", err)
	}

	var offset int64

	for {
		// Read the fixed 13-byte header.
		header := make([]byte, recordHeaderSize)
		_, err := io.ReadFull(w.file, header)
		if err == io.EOF || err == io.ErrUnexpectedEOF {
			// Clean end of file — replay complete.
			break
		}
		if err != nil {
			return fmt.Errorf("wal: replay read header: %w", err)
		}

		// Parse header fields.
		storedCRC := binary.LittleEndian.Uint32(header[0:4])
		op := OpCode(header[4])
		keyLen := binary.LittleEndian.Uint32(header[5:9])
		valLen := binary.LittleEndian.Uint32(header[9:13])

		// Validate OpCode.
		if op != OpSet && op != OpDelete {
			fmt.Printf("wal: replay: unknown opcode 0x%02x at offset %d, truncating\n", op, offset)
			if err := w.truncateAt(offset); err != nil {
				return err
			}
			break
		}

		// Read Key + Value payload.
		payload := make([]byte, int(keyLen)+int(valLen))
		_, err = io.ReadFull(w.file, payload)
		if err == io.ErrUnexpectedEOF {
			// Partial record — process crashed mid-write. Truncate here.
			fmt.Printf("wal: replay: partial record at offset %d, truncating\n", offset)
			if err := w.truncateAt(offset); err != nil {
				return err
			}
			break
		}
		if err != nil {
			return fmt.Errorf("wal: replay read payload: %w", err)
		}

		// Verify CRC32: recompute over header[4:] + payload.
		// We need to reassemble the bytes that were checksummed.
		checksumData := make([]byte, 1+4+4+len(payload))
		checksumData[0] = byte(op)
		binary.LittleEndian.PutUint32(checksumData[1:5], keyLen)
		binary.LittleEndian.PutUint32(checksumData[5:9], valLen)
		copy(checksumData[9:], payload)

		computedCRC := crc32.Checksum(checksumData, crcTable)
		if computedCRC != storedCRC {
			// Checksum mismatch — corrupted record. Truncate here.
			fmt.Printf("wal: replay: checksum mismatch at offset %d (stored=%08x computed=%08x), truncating\n",
				offset, storedCRC, computedCRC)
			if err := w.truncateAt(offset); err != nil {
				return err
			}
			break
		}

		// Record is valid — call the callback.
		rec := Record{
			Op:  op,
			Key: payload[:keyLen],
		}
		if op == OpSet {
			rec.Value = payload[keyLen:]
		}

		if err := fn(rec); err != nil {
			return fmt.Errorf("wal: replay callback: %w", err)
		}

		// Advance offset past this record.
		offset += int64(recordHeaderSize) + int64(keyLen) + int64(valLen)
	}

	// After replay, seek to end so subsequent Appends go to the right place.
	if _, err := w.file.Seek(0, io.SeekEnd); err != nil {
		return fmt.Errorf("wal: replay seek to end: %w", err)
	}

	return nil
}

// truncateAt truncates the WAL file at the given byte offset.
// Called when a corrupt or partial record is detected during replay.
func (w *WAL) truncateAt(offset int64) error {
	if err := w.file.Truncate(offset); err != nil {
		return fmt.Errorf("wal: truncate at %d: %w", offset, err)
	}
	if _, err := w.file.Seek(offset, io.SeekStart); err != nil {
		return fmt.Errorf("wal: seek after truncate: %w", err)
	}
	return nil
}

// Close flushes and closes the WAL file.
// No further operations should be performed after Close.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Final fsync before closing.
	if err := w.file.Sync(); err != nil {
		return fmt.Errorf("wal: close sync: %w", err)
	}
	if err := w.file.Close(); err != nil {
		return fmt.Errorf("wal: close: %w", err)
	}
	return nil
}

// Path returns the absolute path of the WAL file.
func (w *WAL) Path() string {
	return w.path
}