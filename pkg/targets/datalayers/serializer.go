package datalayers

import (
	"io"

	// "strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

// The placeholder of a nil value in the serialized data point.
var NULL string = "nil"

// A serializer that implements the PointSerializer interface and is used
// by Datalayers to serialize simulated data points during data generation.
type Serializer struct{}

// TODO(niebayes): 尝试按列去序列化数据。按列去序列化的话，我们就可以将文件中的一整行读取到内存，然后将它们都推入到一个 array builder 中，
// 这样似乎可以使用 locality 去加速 arrow record batch 的 build。
func (s *Serializer) Serialize(p *data.Point, w io.Writer) error {
	numTags := len(p.TagKeys())
	numFields := len(p.FieldKeys())

	// Rejects this data point if it does not contain any valid tags or fields.
	if numTags+numFields == 0 {
		return nil
	}

	buf := make([]byte, 0, 256)

	// Appends the timestamp. The timestamp is formatted to nanoseconds.
	buf = serialize.FastFormatAppend(p.Timestamp().UTC().UnixNano(), buf)
	if numTags > 0 && numFields > 0 {
		buf = append(buf, ' ')
	}

	// Appends tags.
	for _, tagValue := range p.TagValues() {
		if tagValue != nil {
			buf = serialize.FastFormatAppend(tagValue, buf)
		} else {
			buf = append(buf, NULL...)
		}
		buf = append(buf, ' ')
	}

	for i, fieldValue := range p.FieldValues() {
		if fieldValue != nil {
			buf = serialize.FastFormatAppend(fieldValue, buf)
		} else {
			buf = append(buf, NULL...)
		}
		if i < len(p.FieldValues())-1 {
			buf = append(buf, ' ')
		}
	}

	// Appends a line separator.
	buf = append(buf, '\n')

	// Writes the serialized data point.
	_, err := w.Write(buf)
	return err
}
