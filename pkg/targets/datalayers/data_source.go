package datalayers

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
)

type dataSource struct {
}

func (ds *dataSource) NextItem() data.LoadedPoint {
	panic("")
}

func (ds *dataSource) Headers() *common.GeneratedDataHeaders {
	panic("")
}
