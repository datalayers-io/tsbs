package datalayers

import (
	"errors"

	"github.com/blagojts/viper"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
)

// Wraps the context used during a benchmark.
type benchmark struct {
	dataSource   targets.DataSource
	batchFactory targets.BatchFactory
	pointIndexer targets.PointIndexer
	processor    targets.Processor
	dBCreator    targets.DBCreator
}

// Initializes all context used during the benchmark.
func NewBenchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, v *viper.Viper) (targets.Benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("datalayers only supports file data source")
	}
	benchmark := benchmark{
		dataSource:   NewDataSource(dataSourceConfig.File.Location),
		batchFactory: NewBatchFactory(),
		pointIndexer: NewPointIndexer(),
		processor:    NewProcessor(),
		dBCreator:    NewDBCreator(),
	}
	return &benchmark, nil
}

// GetDataSource returns the DataSource to use for this Benchmark
func (b *benchmark) GetDataSource() targets.DataSource {
	return b.dataSource
}

// GetBatchFactory returns the BatchFactory to use for this Benchmark
func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return b.batchFactory
}

// GetPointIndexer returns the PointIndexer to use for this Benchmark
func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return b.pointIndexer
}

// GetProcessor returns the Processor to use for this Benchmark
func (b *benchmark) GetProcessor() targets.Processor {
	return b.processor
}

// GetDBCreator returns the DBCreator to use for this Benchmark
func (b *benchmark) GetDBCreator() targets.DBCreator {
	return b.dBCreator
}
