# VaultKV

A disk-persistent key-value storage engine built from scratch in Go — implementing a Write-Ahead Log (WAL) for crash recovery and an LSM-Tree for high-throughput writes.

This is not a tutorial follow-along. Every component — the WAL, MemTable skip list, SSTable binary format, Bloom filter, compaction engine, and TCP server — is implemented from first principles without using any existing database libraries.

```
$ ./bin/vaultkv-server --port 6380 --data ./data --sync full

$ ./bin/vaultkv-cli --addr localhost:6380
> SET city atlanta
+OK
> GET city
+atlanta
> PING
+PONG
```

---

## Why I built this

Most backend engineers use key-value stores like Redis or RocksDB without understanding why they are designed the way they are. After spending two years optimizing backend query performance in production systems, I wanted to understand storage engines at a fundamental level — not just how to use them, but why every design decision was made.

VaultKV was built to answer three specific questions:

1. **Why do databases use a Write-Ahead Log?** What exactly does it protect against, and what is the recovery cost?
2. **Why do LSM-Trees outperform B-Trees for write-heavy workloads?** What are the precise tradeoffs in read amplification, write amplification, and space amplification?
3. **How does the Bloom filter eliminate disk reads?** What is the real performance difference with and without it?

---

## Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                       TCP Server                            │
│              GET / SET / DEL / PING / STAT                  │
└────────────────────────┬────────────────────────────────────┘
                         │
┌────────────────────────▼────────────────────────────────────┐
│                    Storage Engine                           │
│                                                             │
│  ┌──────────┐   ┌──────────────┐   ┌──────────────────┐    │
│  │   WAL    │──▶│  MemTable    │──▶│ Immutable        │    │
│  │(append-  │   │ (skip list)  │   │ MemTable         │    │
│  │  only)   │   │              │   │ (flushing)       │    │
│  └──────────┘   └──────────────┘   └────────┬─────────┘    │
│                                             │ async flush   │
│  ┌──────────────────────────────────────────▼───────────┐   │
│  │                SSTable Files (disk)                  │   │
│  │  data blocks + sparse index + Bloom filter + footer  │   │
│  └──────────────────────────────────────────────────────┘   │
│                         ▲                                   │
│          Background Size-Tiered Compaction Engine           │
└─────────────────────────────────────────────────────────────┘
```

### Write path
1. Append record to WAL (fsync according to SyncMode)
2. Insert into active MemTable (skip list, O(log n))
3. If MemTable exceeds size threshold → rotate to immutable, flush to SSTable (async)
4. Return `+OK` to client after WAL write — not after flush

### Read path
1. Active MemTable (O(log n) skip list lookup)
2. Immutable MemTable if a flush is in progress
3. SSTable files newest-first: Bloom filter → sparse index binary search → data block read

---

## Benchmark Results

**Hardware:** AMD Ryzen 5 4600H · Ubuntu 24.04 · NVMe SSD  
**Methodology:** `go test -bench=. -benchtime=10s` · median of results

### Write throughput

| Implementation | ops/sec | ns/op | vs Naive |
|----------------|---------|-------|----------|
| Naive (append to file per write) | ~92,500 | 10,804 | baseline |
| VaultKV — SyncMode: full (fsync per write) | ~296,000 | 3,377 | **3.2× faster** |
| VaultKV — SyncMode: none (OS-managed) | ~367,000 | 2,722 | **4.0× faster** |

The naive baseline opens, writes, fsyncs, and closes a file on every operation — the simplest possible persistent KV store. VaultKV's append-only WAL and MemTable buffering significantly outperform it even with full fsync.

### Read throughput

| Operation | ops/sec | ns/op | Notes |
|-----------|---------|-------|-------|
| MemTable hit (hot key) | ~1,859,000 | 538 | Skip list O(log n) |
| SSTable hit (disk read) | ~235,000 | 4,254 | Index lookup + block read |
| SSTable absent — Bloom filter | ~3,602,000 | 278 | **No disk read** |

**Key insight:** When a key does not exist in an SSTable, the Bloom filter resolves the query in 278ns with zero disk I/O — 15× faster than a full disk read at 4,254ns.

### Bloom filter

| Operation | ops/sec | ns/op |
|-----------|---------|-------|
| MayContain (key present) | ~13,000,000 | 76.7 |
| MayContain (key absent) | ~24,360,000 | 41.1 |
| Build (100K keys) | — | 7.35ms |

False positive rate at 9.6 bits/key with k=3 hash functions: **1.95%**  
This means 98.05% of absent-key SSTable lookups skip disk I/O entirely.

### WAL performance

| Mode | ops/sec | ns/op | Max data loss on crash |
|------|---------|-------|----------------------|
| AppendSync (fsync per write) | ~562,000 | 1,780 | Zero |
| AppendNoSync (OS-managed) | ~741,000 | 1,350 | Seconds |
| Replay (100K records) | — | 221ms | — |

### Compaction

| Operation | time | Notes |
|-----------|------|-------|
| RunOnce (merge 4 × 1000-entry SSTables) | 2.33ms | Size-tiered, N-way merge heap |

---

## Core Components

### Write-Ahead Log (WAL)
Append-only binary log providing crash recovery. Every write is recorded before touching the MemTable. On startup, the WAL is replayed to recover committed writes.

Binary record format:
```
[CRC32 uint32][OpCode byte][KeyLen uint32][ValLen uint32][Key bytes][Value bytes]
```

CRC32 checksum covers everything after the checksum field. Partial records (crash mid-write) are detected by checksum mismatch and truncated cleanly.

Three sync modes:
- `full` — fsync per write: zero data loss, ~296K writes/sec
- `batched` — fsync every 100ms: ≤100ms data loss, higher throughput
- `none` — OS-managed: maximum throughput, seconds of potential data loss

### MemTable (Skip List)
In-memory sorted buffer for recent writes. Uses a 12-level skip list with promotion probability p=0.25, giving O(log n) average insert and lookup. Wrapped with `sync.RWMutex` for concurrent access — multiple readers, single writer.

When `ByteSize()` exceeds `Options.MemTableSize` (default 4MB), the MemTable rotates to immutable and is flushed to an SSTable in a background goroutine.

### SSTable File Format
Immutable sorted files on disk. Never modified after creation — only replaced during compaction.

```
┌─────────────────────────────────────────┐
│  Data Blocks                            │
│  [key_len][val_len][flags][key][value]  │
│  + CRC32 trailer per block              │
├─────────────────────────────────────────┤
│  Index Block (sparse — one per block)   │
│  [key_len][first_key][offset][len]      │
├─────────────────────────────────────────┤
│  Bloom Filter Block                     │
│  [k][m][bit_array]                      │
├─────────────────────────────────────────┤
│  Footer (48 bytes)                      │
│  [index_offset][index_len]              │
│  [bloom_offset][bloom_len]              │
│  [num_entries][magic][version]          │
└─────────────────────────────────────────┘
```

Magic number `0x5661756C744B5600` ("VaultKV\0") validates file integrity on open.

### Bloom Filter
One Bloom filter per SSTable eliminates 98% of unnecessary disk reads for absent keys. Implemented from scratch using MurmurHash3 with three independent seeds.

At default settings (9.6 bits/key, k=3):
- False positive rate: **1.95%** (measured)
- False negative rate: **0%** (mathematically guaranteed)
- Memory overhead: ~120 bytes per 100 keys

### Compaction Engine
Size-tiered compaction runs in a background goroutine. When a size tier accumulates ≥4 SSTables, they are merged into one using an N-way merge heap (`container/heap`). Duplicate keys keep the newest version. Tombstones are eliminated during full-tier compaction.

The amplification triad — no strategy minimises all three simultaneously:

| Strategy | Write Amp | Read Amp | Space Amp | Best for |
|----------|-----------|----------|-----------|----------|
| Size-Tiered (default) | 1.4× | 8× | 2.1× | Write-heavy |
| Level-Based | 3.2× | 1.8× | 1.1× | Read-heavy |

---

## Getting Started

### Prerequisites
- Go 1.22+
- Linux, macOS, or Windows (WSL2)

### Install

```bash
git clone https://github.com/DhairyaParikh31/vaultkv
cd vaultkv
go mod tidy
go test ./...
```

### Build

```bash
go build -o bin/vaultkv-server ./cmd/server
go build -o bin/vaultkv-cli ./cmd/cli
```

### Run

```bash
# Terminal 1 — start server
./bin/vaultkv-server --port 6380 --data ./data --sync full

# Terminal 2 — connect CLI
./bin/vaultkv-cli --addr localhost:6380
```

### Embed as a library

```go
db, err := vaultkv.Open(vaultkv.Options{
    Dir:          "./data",
    SyncMode:     vaultkv.SyncFull,
    MemTableSize: 4 * 1024 * 1024, // 4MB
})
defer db.Close()

db.Set([]byte("hello"), []byte("world"))
val, err := db.Get([]byte("hello"))
db.Delete([]byte("hello"))
```

### TCP Protocol

```bash
# Connect with netcat — no client needed
echo -e "SET foo bar\r\nGET foo\r\n" | nc localhost 6380
```

| Command | Request | Response |
|---------|---------|----------|
| SET | `SET <key> <value>\r\n` | `+OK\r\n` |
| GET | `GET <key>\r\n` | `+<value>\r\n` or `-ERR not found\r\n` |
| DEL | `DEL <key>\r\n` | `+OK\r\n` |
| PING | `PING\r\n` | `+PONG\r\n` |
| STAT | `STAT\r\n` | `+{json stats}\r\n` |

---

## Configuration

| Option | Default | Description |
|--------|---------|-------------|
| `Dir` | required | Data directory for WAL and SSTables |
| `SyncMode` | `full` | WAL fsync: `full`, `batched`, `none` |
| `SyncIntervalMs` | `100` | Fsync interval in batched mode (ms) |
| `MemTableSize` | `4MB` | MemTable rotation threshold |
| `CompactionStrategy` | `size-tiered` | `size-tiered` or `level-based` |
| `SizeTieredThreshold` | `4` | Files per tier before compaction triggers |
| `BloomFPRate` | `0.01` | Bloom filter false positive rate |
| `BlockSize` | `4096` | SSTable data block size (bytes) |

---

## Project Structure

```
vaultkv/
├── cmd/
│   ├── server/main.go       # TCP server
│   └── cli/main.go          # Interactive CLI client
├── internal/
│   ├── wal/                 # Write-Ahead Log
│   ├── memtable/            # Skip list MemTable
│   ├── sstable/             # SSTable writer + reader
│   ├── bloom/               # Bloom filter + MurmurHash3
│   └── compaction/          # Size-tiered compaction engine
├── db.go                    # Public API
├── options.go               # Configuration
└── README.md
```

---

## Limitations

VaultKV is a research and learning project. It is not production-ready.

- Single node only — no replication or distributed consensus
- No ACID transactions — individual operations are atomic
- No authentication — TCP server has no access control
- No value compression
- No point-in-time recovery — WAL truncated after SSTable flush

---

## Related projects

- [Sluice](https://github.com/DhairyaParikh31/sluice) — HTTP reverse proxy with token bucket rate limiting and live metrics
- [CruxDB](https://github.com/DhairyaParikh31/cruxdb) — In-memory SQL database with hand-written parser and B-tree indexing

---

## License

MIT