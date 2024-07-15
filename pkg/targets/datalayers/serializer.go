package datalayers

import (
	"io"
	"log"
	"strconv"
	"strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

// The placeholder of a nil value in the serialized data point.
var NULL string = "nil"

// A serializer that implements the PointSerializer interface and is used
// by Datalayers to serialize simulated data points during data generation.
type Serializer struct{}

// TODO(niebayes): for each measurement, only the first record contains field keys.
// The serialized data point conforms to the following format:
// <measurement> <timestamp> <field name>=<field value> <field name>=<field value> ...
//
// All tags will be converted to fields since Datalayers has no concept of tags.
//
// Currently, the TSBS benchmark suite only supports bool, i32, i64, f32, f64, binary, string types.
// As a consequence, the serializer for Datalayers only supports serializing those types.
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	numTags := len(p.TagKeys())
	numFields := len(p.FieldKeys())

	// Rejects this data point if it does not contain any valid key values.
	if numTags+numFields == 0 {
		return nil
	}

	buf := make([]byte, 0, 256)
	dataTypes := make([]string, 0, 16)

	// Appends the measurement name.
	buf = serialize.FastFormatAppend(p.MeasurementName(), buf)
	buf = append(buf, ' ')

	// Appends the timestamp. The timestamp is formatted to nanoseconds.
	buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixNano(), buf)
	buf = append(buf, ' ')

	// Appends tags.
	buf, dataTypes = appendKeyValues(p.TagKeys(), p.TagValues(), buf, dataTypes)
	if numTags > 0 && numFields > 0 {
		buf = append(buf, ' ')
	}

	// Appends fields.
	buf, dataTypes = appendKeyValues(p.FieldKeys(), p.FieldValues(), buf, dataTypes)

	// Appends data types.
	if len(dataTypes) > 0 {
		buf = append(buf, ' ')
		compressedDataTypes := strings.Join(dataTypes, ",")
		buf = serialize.FastFormatAppend(compressedDataTypes, buf)
	}

	// Appends a line separator.
	buf = append(buf, '\n')

	// Writes the serialized data point.
	_, err := w.Write(buf)
	return err
}

// Appends all key-value pairs into the buffer and returns the extended buffer.
func appendKeyValues(keys [][]byte, values []interface{}, buf []byte, dataTypes []string) ([]byte, []string) {
	for i := 0; i < len(keys); i++ {
		k := keys[i]
		v := values[i]

		buf = serialize.FastFormatAppend(k, buf)
		buf = append(buf, '=')
		if v != nil {
			buf = serialize.FastFormatAppend(v, buf)
		} else {
			buf = append(buf, NULL...)
		}

		if i < len(keys)-1 {
			buf = append(buf, ' ')
		}

		dataType := getDataType(v)
		dataTypes = append(dataTypes, strconv.Itoa(int(dataType)))
	}
	return buf, dataTypes
}

func getDataType(v interface{}) DataType {
	switch v.(type) {
	case nil:
		return DataTypeNil
	case bool:
		return DataTypeBool
	case int, int32:
		return DataTypeInt32
	case int64:
		return DataTypeInt64
	case float32:
		return DataTypeFloat32
	case float64:
		return DataTypeFloat64
	case []byte:
		return DataTypeBinary
	case string:
		return DataTypeString
	default:
		log.Panicf("unexpected data type. value: %v", v)
	}
	return DataTypeNil
}
