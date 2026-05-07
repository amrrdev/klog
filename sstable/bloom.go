package sstable

import (
	"encoding/binary"
	"hash/fnv"
	"math"
)

// bloomFilter is a space-efficient probabilistic data structure for
// testing set membership. It guarantees no false negatives and a
// tunable false positive rate.
type bloomFilter struct {
	bits []byte
	m    uint64
	k    uint64
}

// newBloomFilter creates a bloom filter sized for n expected keys
// at the given false positive rate (e.g. 0.01 for 1%).
//
// Formula for m (number of bits):
//
//	m = -(n × ln(p)) / (ln(2))²
//
// Formula for k (number of hash functions):
//
//	k = (m/n) × ln(2)
//
// At p=0.01 and n keys: m ≈ 10n bits, k ≈ 7 hash functions.
func newBloomFilter(n int, falsePositiveRate float64) *bloomFilter {
	m := uint64(math.Ceil(-float64(n) * math.Log(falsePositiveRate) / (math.Log(2) * math.Log(2))))
	k := uint64(math.Ceil(float64(m) / float64(n) * math.Log(2)))
	m = ((m + 7) / 8) * 8

	return &bloomFilter{
		bits: make([]byte, m/8),
		m:    m,
		k:    k,
	}
}

// hash128 produces two independent 64-bit hashes of key using FNV-1a.
// These two hashes are used as the basis for double hashing.
// FNV-1a is fast and has good distribution for short keys.
func hash128(key []byte) (uint64, uint64) {
	h1 := fnv.New64()
	h1.Write(key)
	v1 := h1.Sum64()

	h2 := fnv.New64a()
	h2.Write(key)
	v2 := h2.Sum64()

	return v1, v2
}

// Double hashing: hash_i = (h1 + i*h2) % m
func (bf *bloomFilter) add(key []byte) {
	h1, h2 := hash128(key)
	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		bf.bits[pos/8] |= 1 << (pos % 8)
	}
}

// mayContain returns false if the key is definitely not in the filter,
// true if it is probably in the filter (may be a false positive).
func (bf *bloomFilter) mayContain(key []byte) bool {
	h1, h2 := hash128(key)
	for i := uint64(0); i < bf.k; i++ {
		pos := (h1 + i*h2) % bf.m
		if bf.bits[pos/8]&(1<<(pos%8)) == 0 {
			return false
		}
	}
	return true
}

// Layout: [k:8][m:8][bits:m/8]
func (bf *bloomFilter) encode() []byte {
	buf := make([]byte, 16+len(bf.bits))
	binary.LittleEndian.PutUint64(buf[0:8], bf.k)
	binary.LittleEndian.PutUint64(buf[8:16], bf.m)
	copy(buf[16:], bf.bits)
	return buf
}

func decodeBloomFilter(data []byte) *bloomFilter {
	k := binary.LittleEndian.Uint64(data[0:8])
	m := binary.LittleEndian.Uint64(data[8:16])
	bits := make([]byte, m/8)
	copy(bits, data[16:])
	return &bloomFilter{bits: bits, m: m, k: k}
}
