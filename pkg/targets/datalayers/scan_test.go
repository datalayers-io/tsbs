package datalayers

import (
	"testing"

	"github.com/timescale/tsbs/pkg/data"
)

func TestHostnameBytes(t *testing.T) {
	cases := []struct {
		line string
		want string
	}{
		{"1451606400000000000 host_0 us-west-1 us-west-1b 90", "host_0"},
		{"1451606400000000000 host_2987 us-west-2 us-west-2a", "host_2987"},
		{"1451606400000000000", ""},
	}
	for _, c := range cases {
		got := string(hostnameBytes([]byte(c.line)))
		if got != c.want {
			t.Fatalf("hostnameBytes(%q) = %q, want %q", c.line, got, c.want)
		}
	}
}

// TestPointIndexerRoutesByHostname verifies the client routes every line of a
// host to the same worker, and that this worker equals the server partition
// (rustc-hash golden values).
func TestPointIndexerRoutesByHostname(t *testing.T) {
	idx := NewPointIndexer(8)
	lines := []string{
		"1451606400000000000 host_0 us-west-1 us-west-1b 90",
		"1451606410000000000 host_0 us-west-1 us-west-1b 90",
		"1451606400000000000 host_2987 eu-west-1 eu-west-1c 74",
		"1451606400000000000 host_2128 ap-southeast-2 ap-southeast-2b 61",
	}
	for _, l := range lines {
		got := idx.GetIndex(data.LoadedPoint{Data: []byte(l)})
		host := string(hostnameBytes([]byte(l)))
		want := FxPartitionIndex(host, 8)
		if got != want {
			t.Fatalf("index for %q = %d, want %d", l, got, want)
		}
	}
	// Same host, different timestamps -> same worker.
	a := idx.GetIndex(data.LoadedPoint{Data: []byte("1451606400000000000 host_0 x y z")})
	b := idx.GetIndex(data.LoadedPoint{Data: []byte("1451999999000000000 host_0 x y z")})
	if a != b {
		t.Fatalf("host_0 routed to different workers: %d vs %d", a, b)
	}
}
