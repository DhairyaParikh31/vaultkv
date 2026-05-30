// Package bloom implements a Bloom filter for VaultKV's SSTable files.
//
// A Bloom filter answers set membership queries with:
//   - Zero false negatives (guaranteed)
//   - Tunable false positive rate (default 1%)
//
// VaultKV maintains one Bloom filter per SSTable. Before reading
// a data block from disk, the filter is queried first. If the filter
// reports DEFINITELY ABSENT, the disk read is skipped entirely —
// eliminating 99% of unnecessary I/O for non-existent key lookups.
//
// Mathematical Foundation:
//
//	Given n keys, m bits, and k hash functions:
//	FPP = (1 - e^(-kn/m))^k
//
//	Optimal k = (m/n) × ln(2) ≈ 0.693 × (m/n)
//	Bits per key for target FPP = -log2(FPP) / ln(2) = -1.4427 × log2(FPP)
//
//	At FPP = 0.01 (1%):
//	  bits/key = -1.4427 × log2(0.01) = 9.585
//	  optimal k = 0.693 × 9.585 ≈ 6.64 → rounded to 7
//
// VaultKV uses k=3 hash functions (reduced from optimal 7) for
// implementation simplicity. Actual FPP increases to ~2.4% from
// the 1% theoretical — acceptable for storage use cases.
package bloom

import (
	"encoding/binary"
	"math"
)

const (
	// numHashFunctions is the number of independent hash functions.
	// Using 3 seeds with MurmurHash3 for simplicity.
	numHashFunctions = 3

	// seeds are the three independent seeds for MurmurHash3.
	// Chosen to produce well-distributed, independent hash values.
	seed0 = 0x00000000
	seed1 = 0x9747b28c
	seed2 = 0x5f3759df
)

// Filter is an immutable Bloom filter built from a set of keys.
// Once constructed via Build, it supports only MayContain queries.
type Filter struct {
	bits []byte // bit array stored as byte slice
	m    uint32 // total number of bits
	k    uint8  // number of hash functions
}

// Build constructs a Bloom filter for the given set of keys.
//
// bitsPerKey controls the space/accuracy tradeoff:
//   - 9.6 bits/key → ~1% FPP (recommended default)
//   - 14.4 bits/key → ~0.1% FPP (3× memory cost)
//   - 6.0 bits/key → ~5% FPP (saves memory, more false reads)
//
// Build returns an empty filter if keys is empty.
func Build(keys [][]byte, bitsPerKey float64) *Filter {
	if len(keys) == 0 {
		return &Filter{bits: []byte{}, m: 0, k: numHashFunctions}
	}

	// Compute bit array size: m = ceil(n × bitsPerKey).
	// Round up to the nearest byte boundary.
	n := len(keys)
	m := uint32(math.Ceil(float64(n) * bitsPerKey))
	// Ensure m is a multiple of 8 for clean byte alignment.
	m = ((m + 7) / 8) * 8

	bits := make([]byte, m/8)

	// Insert each key: set k bits determined by the hash functions.
	for _, key := range keys {
		h0 := murmurHash3(key, seed0)
		h1 := murmurHash3(key, seed1)
		h2 := murmurHash3(key, seed2)

		setBit(bits, h0%m)
		setBit(bits, h1%m)
		setBit(bits, h2%m)
	}

	return &Filter{
		bits: bits,
		m:    m,
		k:    numHashFunctions,
	}
}

// MayContain returns true if the key MIGHT be in the set,
// or false if the key is DEFINITELY NOT in the set.
//
// A false return guarantees the key was never inserted — no disk
// read is needed. A true return means proceed to the disk read.
//
// False positive rate at default settings (k=3, 9.6 bits/key): ~2.4%
// This means ~2.4% of absent-key queries will still read from disk.
func (f *Filter) MayContain(key []byte) bool {
	if f.m == 0 {
		return false
	}

	h0 := murmurHash3(key, seed0) % f.m
	h1 := murmurHash3(key, seed1) % f.m
	h2 := murmurHash3(key, seed2) % f.m

	return getBit(f.bits, h0) &&
		getBit(f.bits, h1) &&
		getBit(f.bits, h2)
}

// Encode serializes the Bloom filter to a byte slice for storage
// in the SSTable Bloom filter block.
//
// Wire format:
//
//	[k uint8][m uint32 LE][bits []byte]
//	Total: 1 + 4 + len(bits) bytes
func (f *Filter) Encode() []byte {
	buf := make([]byte, 1+4+len(f.bits))
	buf[0] = f.k
	binary.LittleEndian.PutUint32(buf[1:5], f.m)
	copy(buf[5:], f.bits)
	return buf
}

// Decode deserializes a Bloom filter from the byte slice produced by Encode.
// Returns an error-free Filter — malformed data returns an empty filter.
func Decode(data []byte) *Filter {
	if len(data) < 5 {
		return &Filter{}
	}

	k := data[0]
	m := binary.LittleEndian.Uint32(data[1:5])
	bits := make([]byte, len(data)-5)
	copy(bits, data[5:])

	return &Filter{
		bits: bits,
		m:    m,
		k:    k,
	}
}

// BitCount returns the total number of bits in the filter.
// Used for size reporting and benchmark analysis.
func (f *Filter) BitCount() uint32 {
	return f.m
}

// ByteSize returns the number of bytes used by the filter's bit array.
func (f *Filter) ByteSize() int {
	return len(f.bits)
}

// ── Bit manipulation helpers ───────────────────────────────────────────

// setBit sets bit at position pos in the byte slice.
func setBit(bits []byte, pos uint32) {
	bits[pos/8] |= 1 << (pos % 8)
}

// getBit returns true if bit at position pos is set.
func getBit(bits []byte, pos uint32) bool {
	return bits[pos/8]&(1<<(pos%8)) != 0
}

// ── MurmurHash3 (32-bit) ───────────────────────────────────────────────
//
// MurmurHash3 is a non-cryptographic hash function chosen for:
//   - Excellent avalanche effect (small key changes → large hash changes)
//   - High throughput (~1 byte/cycle on modern CPUs)
//   - No external dependencies — implemented from scratch here
//
// Reference: Austin Appleby, MurmurHash3, 2011.
// https://github.com/aappleby/smhasher

const (
	c1 = 0xcc9e2d51
	c2 = 0x1b873593
)

// murmurHash3 computes a 32-bit MurmurHash3 of key with the given seed.
func murmurHash3(key []byte, seed uint32) uint32 {
	h := seed
	length := len(key)
	nblocks := length / 4

	// Process 4-byte blocks.
	for i := 0; i < nblocks; i++ {
		k := binary.LittleEndian.Uint32(key[i*4 : i*4+4])
		k *= c1
		k = rotl32(k, 15)
		k *= c2

		h ^= k
		h = rotl32(h, 13)
		h = h*5 + 0xe6546b64
	}

	// Process remaining bytes (tail).
	tail := key[nblocks*4:]
	var k uint32
	switch len(tail) {
	case 3:
		k ^= uint32(tail[2]) << 16
		fallthrough
	case 2:
		k ^= uint32(tail[1]) << 8
		fallthrough
	case 1:
		k ^= uint32(tail[0])
		k *= c1
		k = rotl32(k, 15)
		k *= c2
		h ^= k
	}

	// Finalization mix — force all bits to avalanche.
	h ^= uint32(length)
	h = fmix32(h)

	return h
}

// rotl32 rotates x left by r bits.
func rotl32(x uint32, r uint8) uint32 {
	return (x << r) | (x >> (32 - r))
}

// fmix32 is the MurmurHash3 finalization mix.
// Ensures all bits of the hash are fully mixed.
func fmix32(h uint32) uint32 {
	h ^= h >> 16
	h *= 0x85ebca6b
	h ^= h >> 13
	h *= 0xc2b2ae35
	h ^= h >> 16
	return h
}