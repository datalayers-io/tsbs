package datalayers

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/prometheus/common/log"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
)

type writeContext struct {
	arrowFields        []arrow.Field
	arrowRecordBuilder *array.RecordBuilder
	// The prepared statement for writing to a table.
	preparedStatement *flightsql.PreparedStatement
}

func NewWriteContext(client *datalayers.Client, p *point) *writeContext {
	numFields := len(p.fields) + 1
	fieldNames := make([]string, numFields)
	placeholders := make([]string, numFields)
	arrowFields := make([]arrow.Field, numFields)

	fieldNames = append(fieldNames, "ts")
	placeholders = append(placeholders, "?")
	arrowFields = append(arrowFields, arrow.Field{
		Name:     "ts",
		Type:     arrow.FixedWidthTypes.Time64ns,
		Nullable: false,
	})

	for _, field := range p.fields {
		fieldNames = append(fieldNames, field.key)
		placeholders = append(placeholders, "?")
		arrowFields = append(arrowFields, arrow.Field{
			Name:     field.key,
			Type:     dataTypeToArrowDataType(field.dataType),
			Nullable: true,
		})
	}

	preparedQuery := fmt.Sprintf("INSERT INTO %v (%v) VALUES (%v)", p.measurement, strings.Join(fieldNames, ","), strings.Join(placeholders, ","))
	log.Infof("the prepared query for table %v is: %v", p.measurement, preparedQuery)

	preparedStatement, err := client.Prepare(preparedQuery)
	if err != nil {
		panic(fmt.Sprintf("failed to create a prepared statement. error: %v", err))
	}

	arrowSchema := arrow.NewSchema(arrowFields, nil)
	// TODO(niebayes): is it good to assign an allocator to each record builder?
	arrowRecordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)

	return &writeContext{arrowFields: arrowFields[1:], arrowRecordBuilder: arrowRecordBuilder, preparedStatement: preparedStatement}
}

func (ctx *writeContext) append(p *point) {
	arrowRecordBuilder := ctx.arrowRecordBuilder
	appendFieldValue(arrowRecordBuilder.Field(0), arrow.FixedWidthTypes.Time64ns, p.timestamp)
	for i, field := range p.fields {
		fieldBuilder := arrowRecordBuilder.Field(i + 1)
		fieldType := dataTypeToArrowDataType(field.dataType)
		fieldValue := field.value
		appendFieldValue(fieldBuilder, fieldType, fieldValue)
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
	case arrow.FixedWidthTypes.Time64ns:
		builder := fieldBuilder.(*array.TimestampBuilder)
		v, err := arrow.TimestampFromString(fieldValue, arrow.Nanosecond)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to timestamp. error: %v", err))
		}
		builder.Append(v)
	}
}

func dataTypeToArrowDataType(dataType DataType) arrow.DataType {
	switch dataType {
	case DataTypeBool:
		return arrow.FixedWidthTypes.Boolean
	case DataTypeInt32:
		return arrow.PrimitiveTypes.Int32
	case DataTypeInt64:
		return arrow.PrimitiveTypes.Int64
	case DataTypeFloat32:
		return arrow.PrimitiveTypes.Float32
	case DataTypeFloat64:
		return arrow.PrimitiveTypes.Float64
	case DataTypeBinary:
		return arrow.BinaryTypes.Binary
	case DataTypeString:
		return arrow.BinaryTypes.String
	}
	panic(fmt.Sprintf("cannot convert data type %v to arrow data type", dataType))
}
