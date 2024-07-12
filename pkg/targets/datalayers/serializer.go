package datalayers

import (
	"github.com/timescale/tsbs/pkg/data"
	"io"
)

type Serializer struct{}

func (s *Serializer) Serialize(p *data.Point, w io.Writer) (err error) {
	panic("not implemented")
}
