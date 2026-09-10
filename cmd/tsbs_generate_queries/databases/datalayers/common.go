package datalayers

import (
	"fmt"
	"runtime"
	"time"

	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/devops"
	// "github.com/timescale/tsbs/cmd/tsbs_generate_queries/uses/iot"
	"github.com/timescale/tsbs/cmd/tsbs_generate_queries/utils"
	"github.com/timescale/tsbs/pkg/query"
)

// BaseGenerator contains settings specific for Datalayers.
type BaseGenerator struct{}

// GenerateEmptyQuery returns an empty query.FlightSqlQuery.
func (g *BaseGenerator) GenerateEmptyQuery() query.Query {
	return query.NewFlightSqlQuery()
}

// fillInQuery fills the query struct with data.
func (g *BaseGenerator) fillInQuery(qi query.Query, humanLabel, humanDesc, sql string) {
	q := qi.(*query.FlightSqlQuery)
	q.HumanLabel = []byte(humanLabel)
	q.HumanDescription = []byte(humanDesc)
	q.RawQuery = []byte(sql)
}

// NewDevops creates a new devops use case query generator.
func (g *BaseGenerator) NewDevops(start, end time.Time, scale int) (utils.QueryGenerator, error) {
	core, err := devops.NewCore(start, end, scale)

	if err != nil {
		return nil, err
	}

	d := &Devops{
		BaseGenerator: g,
		Core:          core,
		vcpu:          runtime.NumCPU(),
	}

	if scale <= 1000 {
		d.scenario = "small"
	} else {
		d.scenario = "large"
	}

	return d, nil
}

// hint generates a set_var(parallel_degree=N) comment hint.
func (d *Devops) hint(p int) string {
	return fmt.Sprintf("/*+ set_var(parallel_degree=%d) */", p)
}

// parallelDegreeFor returns the optimal parallel_degree for a given query key under the current scenario.
func (d *Devops) parallelDegreeFor(queryKey string) int {
	if d.scenario == "small" {
		switch queryKey {
		case "groupby-1host-1h":
			return 1
		case "groupby-1host-12h":
			return 1
		case "groupby-8host-1h-1m":
			return 6
		case "groupby-8host-1h-5m":
			return 8
		case "cpu-max-all-1host":
			return 1
		case "cpu-max-all-8host":
			return 16
		case "double-groupby-1":
			return 2
		case "double-groupby-5":
			return 2
		case "double-groupby-all":
			return 2
		case "high-cpu-1host":
			return 3
		case "high-cpu-all":
			return 32
		case "groupby-orderby-limit":
			return 16
		case "lastpoint":
			return 6
		}
	}

	if d.scenario == "large" {
		switch queryKey {
		case "groupby-1host-1h":
			return 1
		case "groupby-1host-12h":
			return 1
		case "groupby-8host-1h-1m":
			return 8
		case "groupby-8host-1h-5m":
			return 8
		case "cpu-max-all-1host":
			return 1
		case "cpu-max-all-8host":
			return 32
		case "double-groupby-1":
			return 16
		case "double-groupby-5":
			return 16
		case "double-groupby-all":
			return 4
		case "high-cpu-1host":
			return 1
		case "high-cpu-all":
			return 4
		case "groupby-orderby-limit":
			return 4
		case "lastpoint":
			return 8
		}
	}

	return d.vcpu
}

// TODO(niebayes): implement Datalayers' query generator for the iot use case
// NewIoT creates a new iot use case query generator.
// func (g *BaseGenerator) NewIoT(start, end time.Time, scale int) (utils.QueryGenerator, error) {
// 	core, err := iot.NewCore(start, end, scale)

// 	if err != nil {
// 		return nil, err
// 	}

// 	iot := &IoT{
// 		BaseGenerator: g,
// 		Core:          core,
// 	}

// 	return iot, nil
// }
