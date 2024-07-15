package datalayers

import (
	"errors"

	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
)

type datalayersConfig struct {
	sqlEndpoint string `yaml:"sql-endpoint"`
	batchSize   uint   `yaml:"batch-size"`
}

// Wraps the context used during a benchmark.
// The point indexer is constructed on the call GetPointIndexer
// since the maxPartitions is not available for NewBenchmark.
type benchmark struct {
	dataSource   targets.DataSource
	batchFactory targets.BatchFactory
	processor    targets.Processor
	dBCreator    targets.DBCreator
}

// Initializes all context used during the benchmark.
func NewBenchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, datalayersConfig *datalayersConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("datalayers only supports file data source")
	}

	datalayersClient, err := datalayers.NewClient(datalayersConfig.sqlEndpoint)
	if err != nil {
		return nil, err
	}

	benchmark := benchmark{
		dataSource:   NewDataSource(dataSourceConfig.File.Location),
		batchFactory: NewBatchFactory(datalayersConfig.batchSize),
		processor:    NewProcessor(datalayersClient, targetDB),
		dBCreator:    NewDBCreator(datalayersClient),
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
	return NewPointIndexer(maxPartitions)
}

// GetProcessor returns the Processor to use for this Benchmark
func (b *benchmark) GetProcessor() targets.Processor {
	return b.processor
}

// GetDBCreator returns the DBCreator to use for this Benchmark
func (b *benchmark) GetDBCreator() targets.DBCreator {
	return b.dBCreator
}
