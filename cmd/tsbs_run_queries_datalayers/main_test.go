package main

import "testing"

func TestParseSwitchHint(t *testing.T) {
	cases := []struct {
		raw     string
		key     string
		present bool
		value   bool
	}{
		{"SELECT /*+ set_var(fast_last_buckets=1) */ x", "fast_last_buckets", true, true},
		{"SELECT /*+ set_var(fast_last_buckets=true) */ x", "fast_last_buckets", true, true},
		{"SELECT /*+ set_var(fast_last_buckets=TRUE) */ x", "fast_last_buckets", true, true},
		{"SELECT /*+ set_var(fast_last_buckets=on) */ x", "fast_last_buckets", true, true},
		{"SELECT /*+ set_var(fast_last_buckets=0) */ x", "fast_last_buckets", true, false},
		{"SELECT /*+ set_var(fast_last_buckets=false) */ x", "fast_last_buckets", true, false},
		{"SELECT /*+ set_var(fast_last_buckets=OFF) */ x", "fast_last_buckets", true, false},
		{"SELECT /*+ set_var(fast_last_buckets = true) */ x", "fast_last_buckets", true, true},
		{"SELECT /*+ set_var(skip_rollup=true) */ x", "skip_rollup", true, true},
		{"SELECT /*+ set_var(parallel_degree=4) */ x", "fast_last_buckets", false, false},
		{"SELECT x", "fast_last_buckets", false, false},
	}
	for _, c := range cases {
		present, value := parseSwitchHint(c.raw, c.key)
		if present != c.present || value != c.value {
			t.Errorf("parseSwitchHint(%q, %q) = (%v, %v), want (%v, %v)",
				c.raw, c.key, present, value, c.present, c.value)
		}
	}
}
