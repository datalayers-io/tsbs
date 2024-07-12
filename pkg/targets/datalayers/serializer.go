package datalayers

import (
	"io"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

// The placeholder of a nil value in the serialized data point.
var NULL string = "nil"

// A serializer that implements the PointSerializer interface and is used
// by Datalayers to serialize simulated data points during data generation.
type Serializer struct{}

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

	// Appends the measurement name.
	buf = serialize.FastFormatAppend(p.MeasurementName(), buf)
	buf = append(buf, ' ')

	// Appends the timestamp. The timestamp is formatted to nanoseconds.
	buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixNano(), buf)
	buf = append(buf, ' ')

	// Appends tags.
	buf = appendKeyValues(p.TagKeys(), p.TagValues(), buf)
	if numTags > 0 && numFields > 0 {
		buf = append(buf, ' ')
	}

	// Appends fields.
	buf = appendKeyValues(p.FieldKeys(), p.FieldValues(), buf)
	buf = append(buf, '\n')

	// Writes the serialized data point.
	_, err := w.Write(buf)
	return err
}

// Appends all key-value pairs into the buffer and returns the extended buffer.
func appendKeyValues(keys [][]byte, values []interface{}, buf []byte) []byte {
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
	}
	return buf
}
