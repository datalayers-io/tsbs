package datalayers

import (
	"bufio"
	"fmt"
	"os"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// DataSourceFile is the underlying data file, kept so the generic loader can
// close it after the scan finishes.
var DataSourceFile *os.File = nil

// dataSource reads the data file line by line using a buffered scanner,
// so that no line is ever split across a chunk boundary.
type dataSource struct {
	scanner *bufio.Scanner
}

// NewDataSource creates a sequential, line-oriented file data source.
func NewDataSource(fileName string, numProcessors int64) targets.DataSource {
	file, err := os.Open(fileName)
	if err != nil {
		panic(fmt.Sprintf("failed to open file %v. error: %v", fileName, err))
	}
	DataSourceFile = file

	scanner := bufio.NewScanner(file)
	// Lines are ~100 bytes; raise the cap well above the default 64KB to be safe.
	scanner.Buffer(make([]byte, 0, 64*1024), 4*1024*1024)

	return &dataSource{scanner: scanner}
}

// NextItem retrieves the next line from the data source.
func (ds *dataSource) NextItem() data.LoadedPoint {
	if !ds.scanner.Scan() {
		return data.LoadedPoint{}
	}
	line := make([]byte, len(ds.scanner.Bytes()))
	copy(line, ds.scanner.Bytes())
	return data.LoadedPoint{Data: line}
}

// Gets the headers of the data source. Not used by Datalayers.
func (ds *dataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to.
type pointIndexer struct {
	cursor        uint
	maxPartitions uint
}

// Creates a new point indexer.
func NewPointIndexer(maxPartitions uint) targets.PointIndexer {
	return &pointIndexer{cursor: 0, maxPartitions: maxPartitions}
}

// GetIndex returns a partition for the given Point
func (indexer *pointIndexer) GetIndex(_ data.LoadedPoint) uint {
	index := indexer.cursor % indexer.maxPartitions
	indexer.cursor += 1
	return index
}

// Batch is an aggregate of points for a particular data system.
type batch struct {
	lines [][]byte
}

// Gets the current length of the batch.
func (b *batch) Len() uint {
	return uint(len(b.lines))
}

// Appends a data point (a single line) to the batch.
func (b *batch) Append(loadedPoint data.LoadedPoint) {
	b.lines = append(b.lines, loadedPoint.Data.([]byte))
}

// BatchFactory returns a new empty batch for storing points.
type batchFactory struct {
}

// Creates a new batch factory.
func NewBatchFactory() targets.BatchFactory {
	return &batchFactory{}
}

// New returns a new Batch to add Points to
func (bf *batchFactory) New() targets.Batch {
	return &batch{}
}