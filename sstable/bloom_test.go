package sstable

import (
	"testing"
)

func TestNewBloomFilter(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	if bf == nil {
		t.Fatal("bloom filter should not be nil")
	}
	if bf.m == 0 {
		t.Error("m (number of bits) should be positive")
	}
	if bf.k == 0 {
		t.Error("k (number of hashes) should be positive")
	}
}

func TestNewBloomFilter_Empty(t *testing.T) {
	bf := newBloomFilter(0, 0.01)
	if bf == nil {
		t.Fatal("bloom filter should not be nil")
	}
}

func TestBloomFilter_Add(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	bf.add([]byte("key1"))
	bf.add([]byte("key2"))
	bf.add([]byte("key3"))

	if len(bf.bits) == 0 {
		t.Error("bits should be allocated")
	}
}

func TestBloomFilter_MayContain(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	bf.add([]byte("key1"))

	if !bf.mayContain([]byte("key1")) {
		t.Error("added key should be found")
	}
}

func TestBloomFilter_MayContainFalsePositive(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	bf.add([]byte("key1"))

	if bf.mayContain([]byte("notadded")) {
		t.Log("false positive possible (expected at 1% rate)")
	}
}

func TestBloomFilter_NotContain(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	bf.add([]byte("key1"))

	if bf.mayContain([]byte("key2")) {
		t.Error("non-added key should not be found")
	}
}

func TestBloomFilter_ManyKeys(t *testing.T) {
	bf := newBloomFilter(1000, 0.01)
	for i := 0; i < 1000; i++ {
		key := []byte(string(rune('a' + i%26)))
		bf.add(key)
	}

	for i := 0; i < 1000; i++ {
		key := []byte(string(rune('a' + i%26)))
		if !bf.mayContain(key) {
			t.Errorf("key %d should be found", i)
		}
	}
}

func TestBloomFilter_Encode(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	bf.add([]byte("key1"))

	encoded := bf.encode()
	if len(encoded) == 0 {
		t.Error("encoded should not be empty")
	}
	if len(encoded) < 16 {
		t.Errorf("encoded should have at least 16 bytes header, got %d", len(encoded))
	}
}

func TestBloomFilter_EncodeDecode(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	bf.add([]byte("key1"))
	bf.add([]byte("key2"))
	bf.add([]byte("key3"))

	encoded := bf.encode()
	decoded := decodeBloomFilter(encoded)

	if decoded.k != bf.k {
		t.Errorf("k mismatch: got %d, want %d", decoded.k, bf.k)
	}
	if decoded.m != bf.m {
		t.Errorf("m mismatch: got %d, want %d", decoded.m, bf.m)
	}
}

func TestBloomFilter_EncodeDecodeRoundTrip(t *testing.T) {
	bf := newBloomFilter(100, 0.01)
	for i := 0; i < 50; i++ {
		key := []byte(string(rune('a' + i)))
		bf.add(key)
	}

	encoded := bf.encode()
	decoded := decodeBloomFilter(encoded)

	for i := 0; i < 50; i++ {
		key := []byte(string(rune('a' + i)))
		if !decoded.mayContain(key) {
			t.Errorf("key %d should be found after round trip", i)
		}
	}
}

func TestHash128(t *testing.T) {
	h1, h2 := hash128([]byte("testkey"))
	if h1 == 0 {
		t.Error("h1 should not be zero")
	}
	if h2 == 0 {
		t.Error("h2 should not be zero")
	}
	if h1 == h2 {
		t.Error("h1 and h2 should be different")
	}
}

func TestHash128_Deterministic(t *testing.T) {
	h1a, h2a := hash128([]byte("testkey"))
	h1b, h2b := hash128([]byte("testkey"))

	if h1a != h1b {
		t.Error("h1 should be deterministic")
	}
	if h2a != h2b {
		t.Error("h2 should be deterministic")
	}
}

func TestBloomFilter_DifferentKeys(t *testing.T) {
	bf := newBloomFilter(1000, 0.01)
	keys := []string{"apple", "banana", "cherry", "date", "elderberry"}

	for _, k := range keys {
		bf.add([]byte(k))
	}

	for _, k := range keys {
		if !bf.mayContain([]byte(k)) {
			t.Errorf("key %s should be found", k)
		}
	}
}