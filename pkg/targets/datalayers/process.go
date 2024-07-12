package datalayers

import (
	"github.com/timescale/tsbs/pkg/targets"
)

// Processor is a type that processes the work for a loading worker
type processor struct {
}

func NewProcessor() targets.Processor {
	return &processor{}
}

// Init does per-worker setup needed before receiving data
func (p *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	panic("")
}

// ProcessBatch handles a single batch of data
func (p *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	panic("")
}

// ProcessorCloser is a Processor that also needs to close or cleanup afterwards
//
// Close cleans up after a Processor. Only needed by the ProcessorCloser interface.
func (p *processor) Close(doLoad bool) {
	panic("")
}
