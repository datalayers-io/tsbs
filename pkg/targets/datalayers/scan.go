package datalayers

import (
	"fmt"
	"strings"

	// "log"

	"os"

	// "time"

	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
)

var DataSourceFile *os.File = nil
var HostTags map[string][]string

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
	subFiles [][]int64
	cursor   int
}

// Creates a new file data source.
func NewDataSource(fileName string, numProcessors int64) targets.DataSource {
	file, err := os.Open(fileName)
	if err != nil {
		panic(fmt.Sprintf("failed to open file %v. error: %v", fileName, err))
	}

	fileInfo, err := file.Stat()
	if err != nil {
		panic(fmt.Sprintf("failed to get file info. error: %v", err))
	}
	fileSize := fileInfo.Size()
	// fmt.Printf("The file size is %v\n", fileSize)

	chunkSize := (fileSize + numProcessors - 1) / numProcessors
	// fmt.Printf("The chunk size is %v\n", chunkSize)

	subFiles := make([][]int64, 0, numProcessors)
	for i := int64(0); i < numProcessors; i++ {
		startOffset := i * chunkSize
		endOffset := min(startOffset+chunkSize, fileSize)

		// fmt.Printf("The range of chunk %v is [%v,%v)\n", i, startOffset, endOffset)

		subFiles = append(subFiles, []int64{startOffset, endOffset})
	}

	fmt.Printf("Create %v sub files each of at most length %v for %v processors\n", len(subFiles), chunkSize, numProcessors)

	DataSourceFile = file
	if DataSourceFile == nil {
		panic("The DataSourceFile cannot be nil")
	}

	// Initializes the hostTags map.
	HostTags = make(map[string][]string)

	tagFileName := strings.Replace(fileName, ".data", ".tag", 1)
	content, err := os.ReadFile(tagFileName)
	if err != nil {
		panic(fmt.Sprintf("failed to read the tag file %v. error: %v", tagFileName, err))
	}
	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		tokens := strings.Split(line, " ")
		HostTags[tokens[0]] = tokens[1:]
	}

	return &dataSource{cursor: 0, subFiles: subFiles}
}

// Retrieves the next item from the data source.
// An item only contains a single data point for Datalayers.
func (ds *dataSource) NextItem() data.LoadedPoint {
	if ds.cursor >= len(ds.subFiles) {
		return data.LoadedPoint{}
	}
	subFile := ds.subFiles[ds.cursor]

	// fmt.Printf("Produce a subFile item = %v\n", subFile)

	ds.cursor += 1

	return data.LoadedPoint{Data: subFile}
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
	// fmt.Printf("Create a point indexer with maxPartitions = %v\n", maxPartitions)
	return &pointIndexer{cursor: 0, maxPartitions: maxPartitions}
}

// GetIndex returns a partition for the given Point
func (indexer *pointIndexer) GetIndex(_ data.LoadedPoint) uint {
	index := indexer.cursor % indexer.maxPartitions
	indexer.cursor += 1
	return index
}

// Batch is an aggregate of points for a particular data system.
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type batch struct {
	subFile []int64
}

// Gets the current length of the batch.
// For Datalayers, the length is the number of data points currently stored in the batch.
func (b *batch) Len() uint {
	return 1
}

// Appends a data point to the batch.
func (b *batch) Append(loadedPoint data.LoadedPoint) {
	subFile := loadedPoint.Data.([]int64)
	b.subFile = subFile
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
