package datalayers

import (
	"errors"

	"github.com/prometheus/common/log"
	"github.com/timescale/tsbs/pkg/data/source"
	"github.com/timescale/tsbs/pkg/targets"
	datalayers "github.com/timescale/tsbs/pkg/targets/datalayers/client"
)

type DatalayersConfig struct {
	SqlEndpoint string `yaml:"sql-endpoint" mapstructure:"sql-endpoint"`
	BatchSize   uint   `yaml:"batch-size" mapstructure:"batch-size"`
}

// Wraps the context used during a benchmark.
// The point indexer is constructed on the call GetPointIndexer
// since the maxPartitions is not available for NewBenchmark.
type benchmark struct {
	targetDB         string
	dataSourceConfig *source.DataSourceConfig
	datalayersClient *datalayers.Client
	datalayersConfig *DatalayersConfig
}

// Initializes all context used during the benchmark.
func NewBenchmark(targetDB string, dataSourceConfig *source.DataSourceConfig, datalayersConfig *DatalayersConfig) (targets.Benchmark, error) {
	if dataSourceConfig.Type != source.FileDataSourceType {
		return nil, errors.New("datalayers only supports file data source")
	}

	log.Infof("Read datalayers config:")
	log.Infof("datalayers.sql-endpoint: %v", datalayersConfig.SqlEndpoint)
	log.Infof("datalayers.batch-size: %v", datalayersConfig.BatchSize)

	datalayersClient, err := datalayers.NewClient(datalayersConfig.SqlEndpoint)
	if err != nil {
		return nil, err
	}
	datalayersClient.UseDatabase(targetDB)
	return &benchmark{targetDB, dataSourceConfig, datalayersClient, datalayersConfig}, nil
}

// GetDataSource returns the DataSource to use for this Benchmark
func (b *benchmark) GetDataSource() targets.DataSource {
	return NewDataSource(b.dataSourceConfig.File.Location)
}

// GetBatchFactory returns the BatchFactory to use for this Benchmark
func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	return NewBatchFactory(b.datalayersConfig.BatchSize, &batchPool)
}

// GetPointIndexer returns the PointIndexer to use for this Benchmark
func (b *benchmark) GetPointIndexer(maxPartitions uint) targets.PointIndexer {
	return NewPointIndexer(maxPartitions)
}

// GetProcessor returns the Processor to use for this Benchmark
func (b *benchmark) GetProcessor() targets.Processor {
	return NewProcessor(b.datalayersClient, b.targetDB, &batchPool, int(b.datalayersConfig.BatchSize))
}

// GetDBCreator returns the DBCreator to use for this Benchmark
func (b *benchmark) GetDBCreator() targets.DBCreator {
	return NewDBCreator(b.datalayersClient)
}
