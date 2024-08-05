package datalayers

import (
	"bufio"
	"io"
	"strings"

	// "strings"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/serialize"
)

// The placeholder of a nil value in the serialized data point.
var NULL string = "nil"

// A serializer that implements the PointSerializer interface and is used
// by Datalayers to serialize simulated data points during data generation.
type Serializer struct {
	knownHosts map[string]bool
	tagWriter  *bufio.Writer
}

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
	if numTags > 0 || numFields > 0 {
		buf = append(buf, ' ')
	}

	// Appends tags.
	for i, tagValue := range p.TagValues() {
		skip := false
		switch v := tagValue.(type) {
		case string:
			if strings.HasPrefix(v, "host_") {
				if s.knownHosts[v] {
					buf = serialize.FastFormatAppend(v, buf)
					if numFields > 0 {
						buf = append(buf, ' ')
					}
					skip = true
				} else {
					s.knownHosts[v] = true

					tagBuf := make([]byte, 0, 256)
					for _, tagValue := range p.TagValues() {
						if tagValue != nil {
							tagBuf = serialize.FastFormatAppend(tagValue, tagBuf)
						} else {
							tagBuf = append(tagBuf, NULL...)
						}
						if i < numFields-1 {
							tagBuf = append(tagBuf, ' ')
						}
					}
					tagBuf = append(tagBuf, '\n')
					_, err := s.tagWriter.Write(tagBuf)
					if err != nil {
						panic(err)
					}
					if err := s.tagWriter.Flush(); err != nil {
						panic(err)
					}
				}
			}
		}
		if skip {
			break
		}

		if tagValue != nil {
			buf = serialize.FastFormatAppend(tagValue, buf)
		} else {
			buf = append(buf, NULL...)
		}

		if i < numTags-1 || numFields > 0 {
			buf = append(buf, ' ')
		}
	}

	// Appends fields.
	for i, fieldValue := range p.FieldValues() {
		if fieldValue != nil {
			buf = serialize.FastFormatAppend(fieldValue, buf)
		} else {
			buf = append(buf, NULL...)
		}
		if i < numFields-1 {
			buf = append(buf, ' ')
		}
	}

	// Appends a line separator.
	buf = append(buf, '\n')

	// Writes the serialized data point.
	_, err := w.Write(buf)
	return err
}
