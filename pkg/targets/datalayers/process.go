package datalayers

import (
	"context"
	"fmt"
	"strings"
	"sync"

	// "github.com/prometheus/common/log"
	"github.com/timescale/tsbs/pkg/targets"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
)

// Processor is a type that processes the work for a loading worker
type processor struct {
	targetDB        string
	client          *datalayers.Client
	cpuWriteContext *writeContext
	batchPool       *sync.Pool
}

func NewProcessor(client *datalayers.Client, targetDB string, batchPool *sync.Pool) targets.Processor {
	cpuWriteContext := makeCpuWriteContext(client, targetDB)
	return &processor{targetDB: targetDB, client: client, cpuWriteContext: cpuWriteContext, batchPool: batchPool}
}

// Init does per-worker setup needed before receiving data
func (proc *processor) Init(workerNum int, doLoad, hashWorkers bool) {
	// Not implemented.
}

// ProcessBatch handles a single batch of data
//
// The doLoad parameter is used by the TSBS benchmark suite to test the data parsing and buffering logic.
// If doLoad is true, the processor will load the data batch to the Datalayers server.
// If doLoad is false, no data loading will be performed. Only data parsing and buffering would be performed.
func (proc *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)
	for _, dataSegment := range batch.dataSegments {
		for _, line := range dataSegment.data {
			values := strings.Split(line, " ")
			// Seems the last row of the generate data is not complete.
			if len(values) != len(cpuFieldNames) {
				continue
			}
			proc.cpuWriteContext.appendRaw(values)
		}
	}

	// TODO(niebayes): try to use the naive insert rather than the insert prepared statement.
	if len(batch.dataSegments) > 0 {
		record := proc.cpuWriteContext.flush()

		// Datalayers does not differentiate between tags and fields, all columns are regarded as metrics.
		metricCount += uint64(record.NumCols() * record.NumRows())
		rowCount += uint64(record.NumRows())

		if doLoad {
			proc.cpuWriteContext.preparedStatement.SetParameters(record)
			err := proc.client.ExecuteInsertPrepare(proc.cpuWriteContext.preparedStatement)
			if err != nil {
				panic(fmt.Sprintf("failed to execute a insert prepared statement. error: %v", err))
			}
		}

		record.Release()
	}

	proc.batchPool.Put(batch)

	return metricCount, rowCount
}

// ProcessorCloser is a Processor that also needs to close or cleanup afterwards
//
// Close cleans up after a Processor. Only needed by the ProcessorCloser interface.
func (proc *processor) Close(doLoad bool) {
	proc.cpuWriteContext.arrowRecordBuilder.Release()
	proc.cpuWriteContext.preparedStatement.Close(context.Background())
}
