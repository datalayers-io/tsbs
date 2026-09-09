package datalayers

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/common/log"
	"github.com/timescale/tsbs/pkg/targets"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

var cpuFieldNames []string = []string{
	"ts", "hostname", "region", "datacenter", "rack", "os", "arch", "team", "service", "service_version", "service_environment",
	"usage_user", "usage_system", "usage_idle", "usage_nice", "usage_iowait", "usage_irq", "usage_softirq", "usage_steal",
	"usage_guest", "usage_guest_nice",
}

var cpuFieldTypes []arrow.DataType = []arrow.DataType{
	arrow.FixedWidthTypes.Timestamp_ns,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.BinaryTypes.String,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
	arrow.PrimitiveTypes.Int64,
}

// Processor is a type that processes the work for a loading worker
type processor struct {
	targetDB           string
	batchSize          int
	client             *datalayers.Client
	arrowRecordBuilder *array.RecordBuilder
	preparedStatement  *flightsql.PreparedStatement
}

func NewProcessor(client *datalayers.Client, targetDB string, batchSize int, fileName string) targets.Processor {
	if len(cpuFieldNames) != len(cpuFieldTypes) {
		panic(fmt.Sprintf("len(cpuFieldNames)[%v] != len(cpuFieldTypes)[%v]", len(cpuFieldNames), len(cpuFieldTypes)))
	}

	arrowFields := make([]arrow.Field, 0, len(cpuFieldNames))
	for i := 0; i < len(cpuFieldNames); i++ {
		nullable := true
		// Only the timestamp and hostname fields are not nullable.
		if i == 0 || i == 1 {
			nullable = false
		}
		arrowField := arrow.Field{
			Name:     cpuFieldNames[i],
			Type:     cpuFieldTypes[i],
			Nullable: nullable,
		}
		arrowFields = append(arrowFields, arrowField)
	}

	// Initializes the insert prepared statement.
	preparedStatement, err := client.InsertPrepare(targetDB, "cpu", arrowFields)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize a insert prepared statement for table %v. error: %v", "cpu", err))
	}

	// Initializes the arrow record builder.
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	arrowRecordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)
	arrowRecordBuilder.Reserve(batchSize)

	return &processor{targetDB, batchSize, client, arrowRecordBuilder, preparedStatement}
}

// Init does per-worker setup needed before receiving data
func (proc *processor) Init(workerNum int, doLoad, hashWorkers bool) {
}

// ProcessBatch handles a single batch of data.
//
// The doLoad parameter is used by the TSBS benchmark suite to test the data parsing and buffering logic.
// If doLoad is true, the processor will load the data batch to the Datalayers server.
// If doLoad is false, no data loading will be performed. Only data parsing and buffering would be performed.
func (proc *processor) ProcessBatch(b targets.Batch, doLoad bool) (metricCount, rowCount uint64) {
	batch := b.(*batch)

	for _, line := range batch.lines {
		values := strings.Split(string(line), " ")
		// Skip incomplete rows.
		if len(values) != len(cpuFieldNames) {
			continue
		}
		appendRow(proc.arrowRecordBuilder, values)
		rowCount++
	}

	if rowCount > 0 {
		record := proc.arrowRecordBuilder.NewRecord()

		// Datalayers does not differentiate between tags and fields, all columns are regarded as metrics.
		metricCount += uint64(record.NumCols() * record.NumRows())

		if doLoad {
			proc.preparedStatement.SetParameters(record)
			err := proc.client.ExecuteInsertPrepare(proc.preparedStatement)
			if err != nil {
				log.Error(err)
			}
		}
		record.Release()
	}

	return metricCount, rowCount
}

func appendRow(arrowRecordBuilder *array.RecordBuilder, values []string) {
	for i, value := range values {
		fieldBuilder := arrowRecordBuilder.Field(i)
		if value == NULL {
			fieldBuilder.AppendNull()
		} else {
			appendFieldValue(fieldBuilder, value)
		}
	}
}

func appendFieldValue(fieldBuilder array.Builder, fieldValue string) {
	switch builder := fieldBuilder.(type) {
	case *array.Int64Builder:
		v, err := strconv.ParseInt(fieldValue, 10, 64)
		if err != nil {
			builder.Append(100)
		} else {
			builder.Append(int64(v))
		}
	case *array.StringBuilder:
		builder.Append(fieldValue)
	case *array.TimestampBuilder:
		ts, err := strconv.ParseInt(fieldValue, 10, 64)
		if err != nil {
			builder.AppendTime(time.Unix(0, time.Now().UnixNano()))
		} else {
			builder.AppendTime(time.Unix(0, ts))
		}
	default:
		panic(fmt.Sprintf("unexpected field builder: %v", builder))
	}
}

// ProcessorCloser is a Processor that also needs to close or cleanup afterwards
//
// Close cleans up after a Processor. Only needed by the ProcessorCloser interface.
func (proc *processor) Close(doLoad bool) {
	proc.arrowRecordBuilder.Release()
	proc.preparedStatement.Close(context.Background())
}