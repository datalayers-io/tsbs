package datalayers

import (
	"bufio"

	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

// This file contains stuff used by the scanner.
// When the scanner starts, it spawns a collection of workers.
// There's one duplex channel for each worker so that the scanner could sent data
// to the worker and the worker could send acknowledgement to tell the scanner that
// the sent data are processed.
//
// The scanner also maintains a batch for each channel to buffer scanned data points.
// To determine which channel should a data point go to, we use the point indexer to
// set the index of channels for each data point and send the data point to the corresponding channel.

type dataSource struct {
	scanner *bufio.Scanner
}

// Creates a new file data source.
func NewDataSource(fileName string) targets.DataSource {
	reader := load.GetBufferedReader(fileName)
	// The scanner scans the file line by line.
	scanner := bufio.NewScanner(reader)
	return &dataSource{scanner}
}

// Retrieves the next item from the data source.
// An item only contains a single data point for Datalayers.
func (ds *dataSource) NextItem() data.LoadedPoint {
	panic("")
}

// Gets the headers of the data source. Not used by Datalayers.
func (ds *dataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

// Batch is an aggregate of points for a particular data system.
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type batch struct {
}

// Creates a new batch.
func NewBatch() targets.Batch {
	return &batch{}
}

// Gets the current length of the batch.
// For Datalayers, the length is the number of data points currently stored in the batch.
func (b *batch) Len() uint {
	panic("")
}

// Appends a data point to the batch.
func (b *batch) Append(point data.LoadedPoint) {
	panic("")
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
	panic("")
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to
type pointIndexer struct {
}

// Creates a new point indexer.
func NewPointIndexer() targets.PointIndexer {
	return &pointIndexer{}
}

// GetIndex returns a partition for the given Point
func (indexer *pointIndexer) GetIndex(point data.LoadedPoint) uint {
	panic("")
}
