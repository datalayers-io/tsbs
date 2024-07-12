package datalayers

import (
	"github.com/timescale/tsbs/pkg/targets"
)

type benchmark struct {
}

// GetDataSource returns the DataSource to use for this Benchmark
func (b *benchmark) GetDataSource() targets.DataSource {
	panic("")
}

// GetBatchFactory returns the BatchFactory to use for this Benchmark
func (b *benchmark) GetBatchFactory() targets.BatchFactory {
	panic("")
}

// GetPointIndexer returns the PointIndexer to use for this Benchmark
func (b *benchmark) GetPointIndexer() targets.PointIndexer {
	panic("")
}

// GetProcessor returns the Processor to use for this Benchmark
func (b *benchmark) GetProcessor() targets.Processor {
	panic("")
}

// GetDBCreator returns the DBCreator to use for this Benchmark
func (b *benchmark) GetDBCreator() targets.DBCreator {
	panic("")
}
