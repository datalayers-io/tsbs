package main

import (
	"time"

	"github.com/timescale/tsbs/pkg/query"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
)

type processor struct {
	client           *datalayers.Client
	doPrintResponses bool
}

func newProcessor() query.Processor {
	return &processor{}
}

func (p *processor) Init(_ int) {
	client, err := datalayers.NewClient(sqlEndpoint)
	if err != nil {
		panic(err)
	}
	client.UseDatabase(runner.DBName)

	p.client = client
	p.doPrintResponses = runner.DoPrintResponses()
}

func (p *processor) ProcessQuery(q query.Query, isWarm bool) ([]*query.Stat, error) {
	flightSqlQuery := q.(*query.FlightSqlQuery)
	start := time.Now()

	rawQuery := string(flightSqlQuery.RawQuery)
	if err := p.client.ExecuteQuery(rawQuery, p.doPrintResponses); err != nil {
		return nil, err
	}

	elapsed := float64(time.Since(start).Nanoseconds()) / 1e6
	stat := query.GetStat()
	stat.Init(q.HumanLabelName(), elapsed)
	return []*query.Stat{stat}, nil
}
