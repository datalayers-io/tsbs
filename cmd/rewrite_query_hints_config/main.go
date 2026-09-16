package main

import (
	"encoding/gob"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/yaml.v2"

	"github.com/timescale/tsbs/pkg/query"
)

// usage: rewrite_query_hints_config <config.yaml> <query-dir>
//
// Rewrites every *.query file under <query-dir> (searched recursively),
// replacing the SQL hint comment in each query with the parallel_degree /
// skip_rollup settings from <config.yaml>. The config is keyed by query type,
// i.e. the .query file stem (e.g. "lastpoint"), plus an optional "default"
// section applied to any query type not listed.
//
// Config layout:
//   default:
//     parallel_degree: 8
//     skip_rollup: true
//   queries:
//     single-groupby-1-1-1:
//       parallel_degree: 8
//     lastpoint:
//       parallel_degree: 16
//       skip_rollup: true

var hintRe = regexp.MustCompile(`/\*\+.*?\*/`)

// HintConfig holds the SQL hint settings for one query type.
type HintConfig struct {
	ParallelDegree int  `yaml:"parallel_degree"`
	SkipRollup     bool `yaml:"skip_rollup"`
}

// Config is the top-level YAML layout.
type Config struct {
	Default *HintConfig           `yaml:"default"`
	Queries map[string]HintConfig `yaml:"queries"`
}

// buildHint renders the SQL comment hint from a HintConfig. It returns an
// empty string when neither parallel_degree nor skip_rollup is set, in which
// case the file is left untouched.
func buildHint(hc HintConfig) string {
	var parts []string
	if hc.ParallelDegree > 0 {
		parts = append(parts, fmt.Sprintf("set_var(parallel_degree=%d)", hc.ParallelDegree))
	}
	if hc.SkipRollup {
		parts = append(parts, "set_var(skip_rollup=1)")
	}
	if len(parts) == 0 {
		return ""
	}
	return "/*+ " + strings.Join(parts, ", ") + " */"
}

// rewriteQueries decodes gob-encoded FlightSqlQuery records from r, replaces
// the SQL hint of each one, and re-encodes them to w.
func rewriteQueries(r io.Reader, w io.Writer, hint string) (int, error) {
	dec := gob.NewDecoder(r)
	enc := gob.NewEncoder(w)
	n := 0
	for {
		q := query.NewFlightSqlQuery()
		if err := dec.Decode(q); err != nil {
			if err == io.EOF {
				break
			}
			return n, err
		}
		raw := string(q.RawQuery)
		if hintRe.MatchString(raw) {
			raw = hintRe.ReplaceAllString(raw, hint)
		} else if strings.Contains(raw, "SELECT") {
			raw = strings.Replace(raw, "SELECT", "SELECT "+hint, 1)
		}
		q.RawQuery = []byte(raw)
		if err := enc.Encode(q); err != nil {
			return n, err
		}
		n++
	}
	return n, nil
}

// processFile rewrites path in place (atomic via temp file + rename).
func processFile(path, hint string) (int, error) {
	tmp := path + ".tmp"
	fin, err := os.Open(path)
	if err != nil {
		return 0, err
	}
	fout, err := os.Create(tmp)
	if err != nil {
		fin.Close()
		return 0, err
	}
	n, werr := rewriteQueries(fin, fout, hint)
	cerr := fout.Close()
	fin.Close()
	if werr != nil || cerr != nil {
		os.Remove(tmp)
		if werr != nil {
			return n, werr
		}
		return n, cerr
	}
	if err := os.Rename(tmp, path); err != nil {
		os.Remove(tmp)
		return n, err
	}
	return n, nil
}

func main() {
	if len(os.Args) != 3 {
		panic("usage: rewrite_query_hints_config <config.yaml> <query-dir>")
	}
	cfgPath, dir := os.Args[1], os.Args[2]

	data, err := os.ReadFile(cfgPath)
	if err != nil {
		panic(err)
	}
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		panic(err)
	}

	rewritten, skipped := 0, 0
	err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() || !strings.HasSuffix(d.Name(), ".query") {
			return nil
		}
		stem := strings.TrimSuffix(d.Name(), filepath.Ext(d.Name()))
		var hc HintConfig
		var fromDefault bool
		if c, ok := cfg.Queries[stem]; ok {
			hc = c
		} else if cfg.Default != nil {
			hc = *cfg.Default
			fromDefault = true
		}
		hint := buildHint(hc)
		if hint == "" {
			fmt.Printf("  %-42s unchanged (no hint config)\n", path)
			skipped++
			return nil
		}
		n, err := processFile(path, hint)
		if err != nil {
			return err
		}
		src := "explicit"
		if fromDefault {
			src = "default"
		}
		fmt.Printf("  %-42s rewrote %d queries (%s: hint=%s skip_rollup=%v)\n",
			path, n, src, hint, hc.SkipRollup)
		rewritten++
		return nil
	})
	if err != nil {
		panic(err)
	}
	fmt.Printf("done: %d file(s) rewritten, %d unchanged\n", rewritten, skipped)
}
