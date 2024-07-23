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
	client, err := datalayers.NewClient(sqlEndpoint)
	if err != nil {
		panic(err)
	}
	// TODO(niebayes): do not hardcode.
	client.UseDatabase("benchmark")
	p.client = client
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	flightSqlQuery := q.(*query.FlightSqlQuery)
	start := time.Now()

	// TODO(niebayes): design how to use the prepared statement to speed up processing.
	// where to store prepared statements.
	// how to retrieve back a created prepared statement.
	// is it necessary to use prepared statements?
	// does prepared statements affect the isWarm parameters?

	rawQuery := string(flightSqlQuery.RawQuery)
	if err := p.client.ExecuteQuery(rawQuery); err != nil {
		return nil, err
	}

	elapsed := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), elapsed)
	return []*query.Stat{stat}, nil
}
