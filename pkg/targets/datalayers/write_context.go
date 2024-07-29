package datalayers

import (
	"fmt"
	"strconv"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/memory"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
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

type writeContext struct {
	arrowRecordBuilder *array.RecordBuilder
	// The prepared statement for writing to a table.
	preparedStatement *flightsql.PreparedStatement
}

func makeCpuWriteContext(client *datalayers.Client, dbName string, batchSize int) *writeContext {
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

	// Initializes a insert prepared statement.
	preparedStatement, err := client.InsertPrepare(dbName, "cpu", arrowFields)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize a insert prepared statement for table %v. error: %v", "cpu", err))
	}

	// Initializes an arrow record builder.
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	arrowRecordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)
	arrowRecordBuilder.Reserve(batchSize)

	return &writeContext{arrowRecordBuilder, preparedStatement}
}

func (ctx *writeContext) appendRow(values []string) {
	arrowRecordBuilder := ctx.arrowRecordBuilder
	for i, value := range values {
		fieldBuilder := arrowRecordBuilder.Field(i)
		if value == NULL {
			fieldBuilder.AppendNull()
		} else {
			appendFieldValue(fieldBuilder, value)
		}
	}
}

func (ctx *writeContext) flush() arrow.Record {
	return ctx.arrowRecordBuilder.NewRecord()
}

func appendFieldValue(fieldBuilder array.Builder, fieldValue string) {
	switch builder := fieldBuilder.(type) {
	case *array.Int64Builder:
		if err := builder.AppendValueFromString(fieldValue); err != nil {
			panic(err)
		}
	case *array.StringBuilder:
		builder.Append(fieldValue)
	case *array.TimestampBuilder:
		ts, err := strconv.ParseInt(fieldValue, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to int64. error: %v", err))
		}
		builder.AppendTime(time.Unix(0, ts))
	default:
		panic(fmt.Sprintf("unexpected field builder: %v", builder))
	}
}
