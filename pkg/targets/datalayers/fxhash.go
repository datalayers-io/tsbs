package datalayers

import (
	"encoding/binary"
	"math/bits"
)

// This file ports rustc-hash 2.1.3's FxHasher to Go so that the TSBS Datalayers
// client can route each point to the same worker index that the Datalayers
// server computes for its hash partition.
//
// Server side (src/table_engine/src/partition/partition_computer.rs):
//
//	let mut hasher = rustc_hash::FxHasher::default();
//	for value in pk_values { value.hash(&mut hasher); }   // String -> hasher.write(s.as_bytes())
//	let hash = hasher.finish() as usize;
//	hash & (num_partitions - 1)   // when num_partitions is a power of two
//
// `DatumRef::String(v)` hashes exactly `v.as_bytes()` (see data_types/src/datum.rs),
// so we only need FxHasher over the raw hostname bytes.

// rustc-hash 2.x constants (64-bit target).
const (
	fxK                          uint64 = 0xf1357aea2e62a9c5
	fxSeed1                      uint64 = 0x243f6a8885a308d3
	fxSeed2                      uint64 = 0x13198a2e03707344
	fxPreventTrivialZeroCollapse uint64 = 0xa4093822299f31d0
)

// multiplyMix computes the 64x64 -> 128 product and folds the high half into the
// low half (port of rustc_hash::multiply_mix on 64-bit targets).
func multiplyMix(x, y uint64) uint64 {
	hi, lo := bits.Mul64(x, y)
	return lo ^ hi
}

// hashBytes is a faithful port of rustc_hash::hash_bytes (wyhash-inspired).
func hashBytes(b []byte) uint64 {
	length := len(b)
	s0 := fxSeed1
	s1 := fxSeed2

	if length <= 16 {
		if length >= 8 {
			s0 ^= binary.LittleEndian.Uint64(b[0:8])
			s1 ^= binary.LittleEndian.Uint64(b[length-8:])
		} else if length >= 4 {
			s0 ^= uint64(binary.LittleEndian.Uint32(b[0:4]))
			s1 ^= uint64(binary.LittleEndian.Uint32(b[length-4:]))
		} else if length > 0 {
			lo := uint64(b[0])
			mid := uint64(b[length/2])
			hi := uint64(b[length-1])
			s0 ^= lo
			s1 ^= (hi << 8) | mid
		}
	} else {
		bulk := b[:length-1]
		for len(bulk) >= 16 {
			chunk := bulk[:16]
			x := binary.LittleEndian.Uint64(chunk[0:8])
			y := binary.LittleEndian.Uint64(chunk[8:16])
			t := multiplyMix(s0^x, fxPreventTrivialZeroCollapse^y)
			s0 = s1
			s1 = t
			bulk = bulk[16:]
		}
		suffix := b[length-16:]
		s0 ^= binary.LittleEndian.Uint64(suffix[0:8])
		s1 ^= binary.LittleEndian.Uint64(suffix[8:16])
	}

	return multiplyMix(s0, s1) ^ uint64(length)
}

func fxAddToHash(hash, i uint64) uint64 {
	return (hash + i) * fxK
}

func fxFinish(hash uint64) uint64 {
	// rustc-hash rotates left by 26 bits on 64-bit targets in finish().
	return bits.RotateLeft64(hash, 26)
}

// FxHashBytes replicates `FxHasher::default().write(bytes).finish()`, which is
// exactly what the Datalayers server computes for a string partition key.
func FxHashBytes(b []byte) uint64 {
	h := hashBytes(b)
	h = fxAddToHash(0, h)
	return fxFinish(h)
}

// FxHashString is the string convenience wrapper of FxHashBytes.
func FxHashString(s string) uint64 {
	return FxHashBytes([]byte(s))
}

// FxPartitionIndexBytes maps raw key bytes to a partition index, matching the
// server's `hash & (n-1)` fast path (n power of two) or `hash % n` otherwise.
func FxPartitionIndexBytes(key []byte, partitions uint) uint {
	if partitions == 0 {
		return 0
	}
	h := FxHashBytes(key)
	if partitions&(partitions-1) == 0 {
		return uint(h & uint64(partitions-1))
	}
	return uint(h % uint64(partitions))
}

// FxPartitionIndex is the string convenience wrapper of FxPartitionIndexBytes.
func FxPartitionIndex(hostname string, partitions uint) uint {
	return FxPartitionIndexBytes([]byte(hostname), partitions)
}
