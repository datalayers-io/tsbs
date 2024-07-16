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

type writeContext struct {
	arrowRecordBuilder *array.RecordBuilder
	// The prepared statement for writing to a table.
	preparedStatement *flightsql.PreparedStatement
}

func NewWriteContext(client *datalayers.Client, p *point, dbName string) *writeContext {
	tableName := p.measurement
	numFields := len(p.fields) + 1
	arrowFields := make([]arrow.Field, 0, numFields)

	arrowFields = append(arrowFields, arrow.Field{
		Name:     "ts",
		Type:     arrow.FixedWidthTypes.Timestamp_ns,
		Nullable: false,
	})

	for _, field := range p.fields {
		arrowFields = append(arrowFields, arrow.Field{
			Name:     field.name,
			Type:     field.dataType,
			Nullable: true,
		})
	}

	// TODO(niebayes): support providing partition by fields and partition num through config file.
	// Creates table.
	partitionByFields := []string{"hostname", "region", "datacenter", "rack"}
	partitionNum := uint(64)
	if err := client.CreateTable(dbName, tableName, true, arrowFields, partitionByFields, partitionNum); err != nil {
		panic(fmt.Sprintf("failed to create table %v. error: %v", tableName, err))
	}

	// Initializes a insert prepared statement.
	preparedStatement, err := client.InsertPrepare(dbName, tableName, arrowFields)
	if err != nil {
		panic(fmt.Sprintf("failed to initialize a insert prepared statement for table %v. error: %v", tableName, err))
	}

	// Initializes an arrow record builder.
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	// FIXME(niebayes): is it good to assign an allocator to each record builder?
	arrowRecordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)

	return &writeContext{arrowRecordBuilder, preparedStatement}
}

func (ctx *writeContext) append(p *point) {
	arrowRecordBuilder := ctx.arrowRecordBuilder
	appendFieldValue(arrowRecordBuilder.Field(0), arrow.FixedWidthTypes.Timestamp_ns, p.timestamp)
	for i, field := range p.fields {
		fieldBuilder := arrowRecordBuilder.Field(i + 1)
		if field.value == NULL {
			fieldBuilder.AppendNull()
		} else {
			appendFieldValue(fieldBuilder, field.dataType, field.value)
		}
	}
}

func (ctx *writeContext) flush() arrow.Record {
	return ctx.arrowRecordBuilder.NewRecord()
}

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
