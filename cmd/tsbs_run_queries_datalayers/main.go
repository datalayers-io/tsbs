package main

import (
	"encoding/gob"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

var (
	// The Arrow Flight SQL endpoint exposed by the Datalayers server.
	sqlEndpoint string
	// The runner for running query benchmarks.
	runner *query.BenchmarkRunner
)

var parallelDegreeRe = regexp.MustCompile(`parallel_degree\s*=\s*(\d+)`)

// Session boolean hint (`skip_rollup` / `fast_last_buckets`). Accept the same
// switch spellings the server accepts: 1/0, true/false, on/off (case-insensitive).
var switchHintRe = regexp.MustCompile(`(?i)(skip_rollup|fast_last_buckets)\s*=\s*([a-z0-9]+)`)

// parseSwitchHint returns (present, value) for the requested hint key in raw.
func parseSwitchHint(raw, key string) (bool, bool) {
	for _, m := range switchHintRe.FindAllStringSubmatch(raw, -1) {
		if !strings.EqualFold(m[1], key) {
			continue
		}
		switch strings.ToLower(m[2]) {
		case "1", "true", "on":
			return true, true
		case "0", "false", "off":
			return true, false
		default:
			return false, false
		}
	}
	return false, false
}

func addDatalayersSpecificFlags() {
	pflag.String("sql-endpoint", "127.0.0.1:8360", "The Arrow Flight SQL endpoint exposed by the Datalayers server")
}

func init() {
	// Parse command line args and setup configurations.
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	addDatalayersSpecificFlags()
	pflag.Parse()

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err = viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	// Set the `sqlEndpoint` global variable.
	sqlEndpoint = viper.GetString("sql-endpoint")
	if len(sqlEndpoint) == 0 {
		panic("missing sql endpoint")
	}

	// Initialize the runner.
	runner = query.NewBenchmarkRunner(config)
}

// reportQueryFileHints decodes the query file and prints the session hints
// (parallel_degree / skip_rollup) that its queries carry, so the run can be
// attributed to the correct parallelism.
func reportQueryFileHints(path string) {
	if path == "" {
		return
	}
	f, err := os.Open(path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "cannot open query file %q: %v\n", path, err)
		return
	}
	defer f.Close()

	degrees := map[int]struct{}{}
	skipRollup := map[bool]struct{}{}
	fastLastBuckets := map[bool]struct{}{}
	n := 0
	dec := gob.NewDecoder(f)
	for {
		q := query.NewFlightSqlQuery()
		if err := dec.Decode(q); err != nil {
			break
		}
		raw := string(q.RawQuery)
		if m := parallelDegreeRe.FindStringSubmatch(raw); m != nil {
			if d, err := strconv.Atoi(m[1]); err == nil {
				degrees[d] = struct{}{}
			}
		}
		if present, v := parseSwitchHint(raw, "skip_rollup"); present {
			skipRollup[v] = struct{}{}
		} else {
			skipRollup[false] = struct{}{}
		}
		if present, v := parseSwitchHint(raw, "fast_last_buckets"); present {
			fastLastBuckets[v] = struct{}{}
		} else {
			fastLastBuckets[false] = struct{}{}
		}
		n++
	}

	var ds []int
	for d := range degrees {
		ds = append(ds, d)
	}
	sort.Ints(ds)
	var ss []bool
	for b := range skipRollup {
		ss = append(ss, b)
	}
	sort.Slice(ss, func(i, j int) bool { return !ss[i] && ss[j] })
	var flb []bool
	for b := range fastLastBuckets {
		flb = append(flb, b)
	}
	sort.Slice(flb, func(i, j int) bool { return !flb[i] && flb[j] })

	fmt.Printf("query file %s: %d queries, parallel_degree=%v, skip_rollup=%v, fast_last_buckets=%v\n", path, n, ds, ss, flb)
}

func main() {
	reportQueryFileHints(runner.FileName)
	runner.Run(&query.FlightSqlQueryPool, newProcessor)
}
