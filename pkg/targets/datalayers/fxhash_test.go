package datalayers

import "testing"

// Golden values are taken verbatim from the rustc-hash 2.1.3 unit tests
// (src/lib.rs, `bytes` test). They are `FxBuildHasher.hash_one(HashBytes(bytes))`,
// i.e. `finish(FxHasher::default().write(bytes))`, which is exactly the
// composition the Datalayers server uses for a string partition key.
func TestFxHashBytesVectors(t *testing.T) {
	cases := []struct {
		in   []byte
		want uint64
	}{
		{[]byte(""), 17606491139363777937},
		{[]byte{0}, 5448590020104574886},
		{[]byte{0, 0, 0, 0, 0, 0}, 16766921560080789783},
		{[]byte{1}, 5922447956811044110},
		{[]byte{2}, 5229781508510959783},
		{[]byte("uwu"), 7168164714682931527},
		{[]byte("These are some bytes for testing rustc_hash."), 2349210501944688211},
	}
	for _, c := range cases {
		got := fxFinish(fxAddToHash(0, hashBytes(c.in)))
		if got != c.want {
			t.Fatalf("fx hash mismatch for %q: got %d, want %d", string(c.in), got, c.want)
		}
	}
}

// TestFxPartitionIndexStable pins the hostname -> partition mapping for the
// scenario-2 hostnames and 8/32 partitions. These are the values the Datalayers
// server computes (`FxHasher` over hostname bytes, then `hash & (n-1)`).
func TestFxPartitionIndexStable(t *testing.T) {
	cases := []struct {
		host  string
		parts uint
		want  uint
	}{
		// Golden values produced by rustc-hash 2.1.3 (same version as the server)
		// using `FxHasher::default().write(host.as_bytes()).finish() & (n-1)`,
		// i.e. exactly `DatumRef::String` hashing in the Datalayers server.
		{"host_0", 8, 3},
		{"host_1", 8, 4},
		{"host_2", 8, 6},
		{"host_2987", 8, 2},
		{"host_1668", 8, 7},
		{"host_2750", 8, 3},
		{"host_3423", 8, 4},
		{"host_1345", 8, 4},
		{"host_357", 8, 2},
		{"host_3176", 8, 2},
		{"host_2128", 8, 0},
		{"host_0", 32, 27},
		{"host_1", 32, 20},
		{"host_2987", 32, 2},
	}
	for _, c := range cases {
		got := FxPartitionIndex(c.host, c.parts)
		if got != c.want {
			t.Fatalf("partition index mismatch for %s/%d: got %d, want %d", c.host, c.parts, got, c.want)
		}
	}
}
