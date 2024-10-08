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
	flagSet.String(flagPrefix+"sql-endpoint", "127.0.0.1:8360", "Datalayers' Arrow FlightSql endpoint")
	flagSet.Uint(flagPrefix+"batch-size", 1250, "The number of rows being sent to the Datalayers server in a row")
	flagSet.Uint(flagPrefix+"num-workers", 32, "The number of processors")
}

func (t *datalayersTarget) TargetName() string {
	return constants.FormatDatalayers
}

func (t *datalayersTarget) Serializer() serialize.PointSerializer {
	return &Serializer{}
}

func (t *datalayersTarget) Benchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, dbSpecificViper *viper.Viper) (targets.Benchmark, error) {
	var datalayersConfig DatalayersConfig
	if err := dbSpecificViper.Unmarshal(&datalayersConfig); err != nil {
		return nil, err
	}
	return NewBenchmark(targetDB, dataSourceConfig, &datalayersConfig)
}
