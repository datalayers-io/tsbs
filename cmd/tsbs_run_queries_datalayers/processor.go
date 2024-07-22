package main

import (
	"time"

	"github.com/timescale/tsbs/pkg/query"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
)

type processor struct {
	client *datalayers.Client
}

func newProcessor() query.Processor {
	return &processor{}
}

func (p *processor) Init(_ int) {
	// Not implemented.
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	flightSqlQuery := q.(*query.FlightSqlQuery)
	start := time.Now()

	// TODO(niebayes): design how to use the prepared statement to speed up processing.
	// where to store prepared statements.
	// how to retrieve back a created prepared statement.
	// is it necessary to use prepared statements?
	// does prepared statements affect the isWarm parameters?

	elapsed := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), elapsed)
	return []*query.Stat{stat}, nil
}
