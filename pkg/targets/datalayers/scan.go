package datalayers

import (
	"fmt"
	"sync"
	"time"

	// "log"

	"os"
	"strings"

	// "time"

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

type dataSegment struct {
	data []string
}

type dataSource struct {
	lines  []string
	cursor int
}

func parallelReadFile(fileName string) []byte {
	numReaders := int64(4)

	file, err := os.Open(fileName)
	if err != nil {
		panic(fmt.Sprintf("failed to open file %v. error: %v", fileName, err))
	}
	defer file.Close()

	fileInfo, err := file.Stat()
	if err != nil {
		panic(fmt.Sprintf("failed to get file info. error: %v", err))
	}
	fileSize := fileInfo.Size()

	chunkSize := (fileSize + numReaders - 1) / numReaders

	var wg sync.WaitGroup
	buffers := make([][]byte, numReaders)
	for i := int64(0); i < numReaders; i++ {
		wg.Add(1)
		go func(i int64) {
			defer wg.Done()
			startOffset := i * chunkSize
			endOffset := min(startOffset+chunkSize, fileSize)

			buffer := make([]byte, endOffset-startOffset)
			_, err := file.ReadAt(buffer, startOffset)
			if err != nil {
				panic(fmt.Sprintf("failed to read file at offset %d. error: %v", startOffset, err))
			}
			buffers[i] = buffer
		}(i)
	}
	wg.Wait()

	var flatBuffer []byte
	for _, buffer := range buffers {
		flatBuffer = append(flatBuffer, buffer...)
	}
	return flatBuffer
}

// Creates a new file data source.
// TODO(niebayes): 唯一的生产者线程应该做尽可能轻量化的任务。因为消费者线程数是可调的，所以应该把一些可能的工作交给消费者去处理。
func NewDataSource(fileName string) targets.DataSource {
	// TODO(niebayes): 使用多个 go routines 去读取这个文件以加快准备数据的速度。
	// data, err := os.ReadFile(fileName)
	// if err != nil {
	// 	panic(fmt.Sprintf("failed to read file %v. error: %v", fileName, err))
	// }
	start := time.Now()
	// data := parallelReadFile(fileName)
	data, err := os.ReadFile(fileName)
	if err != nil {
		panic(fmt.Sprintf("failed to read file %v. error: %v", fileName, err))
	}
	elapsed := time.Since(start).Milliseconds()
	fmt.Printf("Reading file costs %v ms\n", elapsed)

	lines := strings.Split(string(data), "\n")
	return &dataSource{lines: lines, cursor: 0}
}

// Retrieves the next item from the data source.
// An item only contains a single data point for Datalayers.
// TODO(niebayes): 修改生产者-消费者分发的逻辑。生产者预先将所有数据按消费者数量去切分，然后通过 round-robin 的 GetIndex 以及 NextItem，
// 一次性将每个消费者应该消费的数据一次性交给他们，然后生产者的任务就结束了，消费者各自为战。虽然实时的写入速度不好看，但是最终的写入速度应该有提升。
func (ds *dataSource) NextItem() data.LoadedPoint {
	if ds.cursor >= len(ds.lines) {
		return data.LoadedPoint{}
	}

	segmentSize := 100
	start := ds.cursor
	end := min(len(ds.lines), ds.cursor+segmentSize)
	segment := ds.lines[start:end]
	ds.cursor += segmentSize

	dataSegment := dataSegment{data: segment}
	return data.LoadedPoint{Data: dataSegment}
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
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type batch struct {
	dataSegments []dataSegment
}

// Gets the current length of the batch.
// For Datalayers, the length is the number of data points currently stored in the batch.
func (b *batch) Len() uint {
	return uint(len(b.dataSegments))
}

// Appends a data point to the batch.
func (b *batch) Append(loadedPoint data.LoadedPoint) {
	dataSegment := loadedPoint.Data.(dataSegment)
	b.dataSegments = append(b.dataSegments, dataSegment)
}

// BatchFactory returns a new empty batch for storing points.
type batchFactory struct {
	capacity  uint
	batchPool *sync.Pool
}

// Creates a new batch factory.
func NewBatchFactory(capacity uint, batchPool *sync.Pool) targets.BatchFactory {
	batchCapacity = capacity
	return &batchFactory{capacity, batchPool}
}

// New returns a new Batch to add Points to
func (bf *batchFactory) New() targets.Batch {
	batch := bf.batchPool.Get().(*batch)
	batch.dataSegments = batch.dataSegments[:0]
	return batch
}

var batchCapacity uint = 100
var batchPool = sync.Pool{
	New: func() interface{} {
		return &batch{
			dataSegments: make([]dataSegment, 0, batchCapacity),
		}
	},
}
