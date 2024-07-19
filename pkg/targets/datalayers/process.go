package datalayers

import (
	"context"
	"fmt"
	"strings"

	// "github.com/prometheus/common/log"
	"github.com/timescale/tsbs/pkg/targets"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
)

// Processor is a type that processes the work for a loading worker
type processor struct {
	targetDB string
	client   *datalayers.Client
	// key: measurement name, aka. table name.
	// value: the context for writing data batch to the table.
	writeContexts map[string]*writeContext
	// The number of partitions for each table.
	partitionNum uint
	// key: measurement name, aka. table name.
	// value: the name of fields to be used as the partition by fields.
	partitionByFields map[string][]string
}

func NewProcessor(client *datalayers.Client, targetDB string, partitionNum uint, rawParitionByFields []string) targets.Processor {
	partitionByFields := make(map[string][]string)
	for _, raw := range rawParitionByFields {
		parts := strings.Split(raw, ":")
		if len(parts) != 2 {
			panic("expect the encoded partition by fields for each table to be `<table name>:<field name>,<field name>...`")
		}
		tableName := strings.TrimSpace(parts[0])
		rawFields := strings.Split(strings.TrimSpace(parts[1]), ",")

		fields := make([]string, 0)
		for _, rawField := range rawFields {
			fields = append(fields, strings.TrimSpace(rawField))
		}

		if len(fields) == 0 {
			panic("the number of partition by fields must be greater than 0")
		}

		partitionByFields[tableName] = fields
	}

	return &processor{targetDB: targetDB, client: client, writeContexts: make(map[string]*writeContext), partitionNum: partitionNum, partitionByFields: partitionByFields}
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
	affected := make(map[string]uint)

	batch := b.(*batch)
	for _, point := range batch.points {
		_, exist := proc.writeContexts[point.measurement]
		if !exist {
			proc.writeContexts[point.measurement] = NewWriteContext(proc.client, &point, proc.targetDB, proc.partitionNum, proc.partitionByFields[point.measurement])
		}
		writeContext := proc.writeContexts[point.measurement]
		writeContext.append(&point)

		affected[point.measurement] += 1
	}

	for measurement, numPoints := range affected {
		writeContext := proc.writeContexts[measurement]
		record := writeContext.flush()

		// Datalayers does not differentiate between tags and fields, all columns are regarded as metrics.
		metricCount += uint64(record.NumCols() * record.NumRows())
		rowCount += uint64(record.NumRows())

		if doLoad {
			_ = numPoints
			// log.Infof("Loading %v points for measurement: %v", numPoints, measurement)

			writeContext.preparedStatement.SetParameters(record)
			err := proc.client.ExecuteInsertPrepare(writeContext.preparedStatement)
			if err != nil {
				panic(fmt.Sprintf("failed to execute a insert prepared statement. error: %v", err))
			}

			// log.Infof("Inserted %v rows to table %v", record.NumRows(), measurement)
		}

		record.Release()
	}

	// log.Infof("processed. metricCount = %v, rowCount = %v", metricCount, rowCount)

	return metricCount, rowCount
}

// ProcessorCloser is a Processor that also needs to close or cleanup afterwards
//
// Close cleans up after a Processor. Only needed by the ProcessorCloser interface.
func (proc *processor) Close(doLoad bool) {
	for _, writeContext := range proc.writeContexts {
		writeContext.arrowRecordBuilder.Release()
		writeContext.preparedStatement.Close(context.Background())
	}
}
