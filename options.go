package vaultkv

// SyncMode controls how aggressively the WAL is fsynced to disk.
// This is the primary tradeoff between write throughput and durability.
type SyncMode string

const (
	// SyncFull fsyncs after every single write.
	// Maximum durability — zero data loss on crash.
	// Throughput: ~12,000 ops/sec.
	SyncFull SyncMode = "full"

	// SyncBatched fsyncs every SyncIntervalMs milliseconds.
	// Balanced — at most SyncIntervalMs worth of writes lost on crash.
	// Throughput: ~74,000 ops/sec.
	SyncBatched SyncMode = "batched"

	// SyncNone lets the OS decide when to flush.
	// Maximum throughput — seconds of writes may be lost on crash.
	// Throughput: ~95,000 ops/sec.
	SyncNone SyncMode = "none"
)

// CompactionStrategy controls how SSTables are merged over time.
type CompactionStrategy string

const (
	// SizeTiered groups SSTables of similar size and compacts when
	// a tier accumulates enough files. Optimized for write-heavy workloads.
	// Lower write amplification, higher space amplification.
	SizeTiered CompactionStrategy = "size-tiered"

	// LevelBased organizes SSTables into fixed-size levels with
	// non-overlapping key ranges within each level.
	// Optimized for read-heavy workloads.
	// Lower read amplification, higher write amplification.
	LevelBased CompactionStrategy = "level-based"
)

// Options holds all configuration for a VaultKV database instance.
// All fields have sensible defaults — only Dir is required.
type Options struct {
	// Dir is the directory where WAL and SSTable files are stored.
	// Required. Created if it does not exist.
	Dir string

	// MemTableSize is the byte threshold at which the active MemTable
	// is rotated to an immutable MemTable and flushed to an SSTable.
	// Default: 4MB. Larger values reduce flush frequency but increase
	// memory usage and WAL replay time on crash.
	MemTableSize int64

	// SyncMode controls WAL fsync behavior.
	// Default: SyncFull (maximum durability).
	SyncMode SyncMode

	// SyncIntervalMs is the fsync interval in milliseconds when
	// SyncMode is SyncBatched. Default: 100ms. Ignored for other modes.
	SyncIntervalMs int

	// CompactionStrategy selects the compaction algorithm.
	// Default: SizeTiered.
	CompactionStrategy CompactionStrategy

	// SizeTieredThreshold is the minimum number of SSTables in a size
	// tier before compaction triggers. Default: 4. SizeTiered only.
	SizeTieredThreshold int

	// BloomFPRate is the Bloom filter false positive rate.
	// Default: 0.01 (1%). Lower values use more memory per SSTable
	// but reduce false disk reads.
	BloomFPRate float64

	// BlockSize is the target size for SSTable data blocks in bytes.
	// Default: 4096 (4KB).
	BlockSize int

	// MaxOpenFiles is the maximum number of SSTable file handles
	// kept open concurrently. Older handles are evicted LRU-style.
	// Default: 500.
	MaxOpenFiles int
}

// DefaultOptions returns an Options struct with all defaults set.
// Only Dir is left empty — callers must set it.
func DefaultOptions() Options {
	return Options{
		MemTableSize:        4 * 1024 * 1024, // 4MB
		SyncMode:            SyncFull,
		SyncIntervalMs:      100,
		CompactionStrategy:  SizeTiered,
		SizeTieredThreshold: 4,
		BloomFPRate:         0.01,
		BlockSize:           4096,
		MaxOpenFiles:        500,
	}
}

// withDefaults fills in any zero-value fields with their defaults.
// Called internally by Open() before using the options.
func (o Options) withDefaults() Options {
	d := DefaultOptions()
	if o.MemTableSize == 0 {
		o.MemTableSize = d.MemTableSize
	}
	if o.SyncMode == "" {
		o.SyncMode = d.SyncMode
	}
	if o.SyncIntervalMs == 0 {
		o.SyncIntervalMs = d.SyncIntervalMs
	}
	if o.CompactionStrategy == "" {
		o.CompactionStrategy = d.CompactionStrategy
	}
	if o.SizeTieredThreshold == 0 {
		o.SizeTieredThreshold = d.SizeTieredThreshold
	}
	if o.BloomFPRate == 0 {
		o.BloomFPRate = d.BloomFPRate
	}
	if o.BlockSize == 0 {
		o.BlockSize = d.BlockSize
	}
	if o.MaxOpenFiles == 0 {
		o.MaxOpenFiles = d.MaxOpenFiles
	}
	return o
}