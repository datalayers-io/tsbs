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

func makeCpuWriteContext(client *datalayers.Client, dbName string) *writeContext {
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
	// FIXME(niebayes): is it good to assign an allocator to each record builder?
	arrowRecordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)

	return &writeContext{arrowRecordBuilder, preparedStatement}
}

// TODO(niebayes): 这里似乎还有一些优化空间
func (ctx *writeContext) appendRaw(values []string) {
	arrowRecordBuilder := ctx.arrowRecordBuilder
	for i, value := range values {
		fieldBuilder := arrowRecordBuilder.Field(i)
		if value == NULL {
			fieldBuilder.AppendNull()
		} else {
			appendFieldValue(fieldBuilder, cpuFieldTypes[i], value)
		}
	}
}

func (ctx *writeContext) flush() arrow.Record {
	return ctx.arrowRecordBuilder.NewRecord()
}

// TODO(niebayes): 给每个 builder 都去预分配空间。
func appendFieldValue(fieldBuilder array.Builder, fieldType arrow.DataType, fieldValue string) {
	switch fieldType {
	case arrow.FixedWidthTypes.Boolean:
		builder := fieldBuilder.(*array.BooleanBuilder)
		v := fieldValue == "true"
		builder.Append(v)
	case arrow.PrimitiveTypes.Int32:
		builder := fieldBuilder.(*array.Int32Builder)
		v, err := strconv.ParseInt(fieldValue, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to int32. error: %v", err))
		}
		builder.Append(int32(v))
	case arrow.PrimitiveTypes.Int64:
		builder := fieldBuilder.(*array.Int64Builder)
		v, err := strconv.ParseInt(fieldValue, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to int64. error: %v", err))
		}
		builder.Append(v)
	case arrow.PrimitiveTypes.Float32:
		builder := fieldBuilder.(*array.Float32Builder)
		v, err := strconv.ParseFloat(fieldValue, 32)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to float32. error: %v", err))
		}
		builder.Append(float32(v))
	case arrow.PrimitiveTypes.Float64:
		builder := fieldBuilder.(*array.Float64Builder)
		v, err := strconv.ParseFloat(fieldValue, 64)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to float64. error: %v", err))
		}
		builder.Append(v)
	case arrow.BinaryTypes.Binary:
		builder := fieldBuilder.(*array.BinaryBuilder)
		v := []byte(fieldValue)
		builder.Append(v)
	case arrow.BinaryTypes.String:
		builder := fieldBuilder.(*array.StringBuilder)
		v := fieldValue
		builder.Append(v)
	case arrow.FixedWidthTypes.Timestamp_ns:
		builder := fieldBuilder.(*array.TimestampBuilder)
		ts, err := strconv.ParseInt(fieldValue, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to int64. error: %v", err))
		}
		v, err := arrow.TimestampFromTime(time.Unix(0, ts).UTC(), arrow.Nanosecond)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to timestamp. error: %v", err))
		}
		builder.Append(v)
	default:
		panic(fmt.Sprintf("unexpected field type: %v", fieldType))
	}
}
