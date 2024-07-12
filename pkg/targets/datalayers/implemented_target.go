package datalayers

import (
	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/pkg/data/serialize"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	"github.com/timescale/tsbs/pkg/targets/constants"
)

type datalayersTarget struct{}

func NewTarget() targets.ImplementedTarget {
	return &datalayersTarget{}
}

func (t *datalayersTarget) TargetSpecificFlags(flagPrefix string, flagSet *pflag.FlagSet) {
	// TODO(niebayes): add datalayer specific flags if necessary.
	// flagSet.String(flagPrefix+"url", "http://localhost:9000/", "QuestDB REST end point")
	// flagSet.String(flagPrefix+"ilp-bind-to", "127.0.0.1:9009", "QuestDB influx line protocol TCP ip:port")
}

func (t *datalayersTarget) TargetName() string {
	return constants.FormatDatalayers
}

func (t *datalayersTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *datalayersTarget) Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	return NewBenchmark(targetDB, dataSourceConfig, v)
}
