package bloom

import (
	"fmt"
	"math/rand"
	"testing"
)

// TestNoFalseNegatives verifies the core guarantee of a Bloom filter:
// a key that was inserted must ALWAYS be reported as MayContain = true.
// False negatives are mathematically impossible in a correct implementation.
func TestNoFalseNegatives(t *testing.T) {
	keys := [][]byte{
		[]byte("John"), []byte("bob"), []byte("charlie"),
		[]byte("database"), []byte("storage"), []byte("vaultkv"),
	}

	f := Build(keys, 9.6)

	for _, key := range keys {
		if !f.MayContain(key) {
			t.Errorf("false negative for key %q — this must never happen", key)
		}
	}
}

// TestFalsePositiveRate measures the actual FPP against 10,000 absent keys.
// With k=3 and 9.6 bits/key, expected FPP ≈ 2.4%.
// We allow up to 5% to account for small-sample variance.
func TestFalsePositiveRate(t *testing.T) {
	// Build filter with 1000 known keys.
	n := 1000
	keys := make([][]byte, n)
	for i := 0; i < n; i++ {
		keys[i] = []byte(fmt.Sprintf("inserted-key-%d", i))
	}
	f := Build(keys, 9.6)

	// Test 10,000 keys that were NOT inserted.
	trials := 10_000
	falsePositives := 0
	for i := 0; i < trials; i++ {
		absent := []byte(fmt.Sprintf("absent-key-%d", i))
		if f.MayContain(absent) {
			falsePositives++
		}
	}

	fpp := float64(falsePositives) / float64(trials)
	t.Logf("False positive rate: %.2f%% (%d/%d)", fpp*100, falsePositives, trials)

	// Allow up to 5% — our k=3 gives ~2.4% theoretical.
	if fpp > 0.05 {
		t.Errorf("false positive rate %.2f%% exceeds 5%% threshold", fpp*100)
	}
}

// TestEmptyFilter verifies that an empty filter always returns false.
func TestEmptyFilter(t *testing.T) {
	f := Build([][]byte{}, 9.6)

	if f.MayContain([]byte("anything")) {
		t.Error("empty filter should never report MayContain = true")
	}
}

// TestSingleKey verifies a filter built from exactly one key.
func TestSingleKey(t *testing.T) {
	key := []byte("only-key")
	f := Build([][]byte{key}, 9.6)

	if !f.MayContain(key) {
		t.Error("inserted key must be found")
	}
}

// TestEncodeDecodeRoundtrip verifies that Encode → Decode preserves
// the filter's behavior exactly.
func TestEncodeDecodeRoundtrip(t *testing.T) {
	keys := [][]byte{
		[]byte("key1"), []byte("key2"), []byte("key3"),
		[]byte("key4"), []byte("key5"),
	}

	original := Build(keys, 9.6)
	encoded := original.Encode()
	decoded := Decode(encoded)

	// All inserted keys must still return true.
	for _, key := range keys {
		if !decoded.MayContain(key) {
			t.Errorf("decoded filter: false negative for %q", key)
		}
	}

	// Bit count must be preserved.
	if original.BitCount() != decoded.BitCount() {
		t.Errorf("bit count mismatch: original=%d decoded=%d",
			original.BitCount(), decoded.BitCount())
	}
}

// TestEncodeDecodeEmptyFilter verifies that an empty filter
// round-trips correctly through Encode/Decode.
func TestEncodeDecodeEmptyFilter(t *testing.T) {
	f := Build([][]byte{}, 9.6)
	decoded := Decode(f.Encode())

	if decoded.MayContain([]byte("anything")) {
		t.Error("decoded empty filter should not contain anything")
	}
}

// TestByteSizeGrowsWithKeys verifies that larger key sets produce
// larger filters, consistent with the bits-per-key formula.
func TestByteSizeGrowsWithKeys(t *testing.T) {
	small := makeKeys(100)
	large := makeKeys(1000)

	fSmall := Build(small, 9.6)
	fLarge := Build(large, 9.6)

	if fLarge.ByteSize() <= fSmall.ByteSize() {
		t.Errorf("larger key set should produce larger filter: small=%d large=%d",
			fSmall.ByteSize(), fLarge.ByteSize())
	}

	t.Logf("100 keys → %d bytes filter", fSmall.ByteSize())
	t.Logf("1000 keys → %d bytes filter", fLarge.ByteSize())
}



// ── Benchmarks ─────────────────────────────────────────────────────────

// BenchmarkBuild benchmarks filter construction for 100K keys.
func BenchmarkBuild(b *testing.B) {
	keys := makeKeys(100_000)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		Build(keys, 9.6)
	}
}

// BenchmarkMayContainHit benchmarks queries for keys that ARE in the filter.
func BenchmarkMayContainHit(b *testing.B) {
	keys := makeKeys(100_000)
	f := Build(keys, 9.6)
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		f.MayContain(keys[i%len(keys)])
	}
}



// ── Helpers ────────────────────────────────────────────────────────────

// makeKeys generates n random unique keys for testing.
func makeKeys(n int) [][]byte {
	rng := rand.New(rand.NewSource(12345))
	keys := make([][]byte, n)
	for i := range keys {
		buf := make([]byte, 16)
		rng.Read(buf)
		keys[i] = []byte(fmt.Sprintf("%x", buf))
	}
	return keys
}
