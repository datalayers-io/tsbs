package datalayers

import (
	"bufio"
	"fmt"
	"log"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/timescale/tsbs/load"
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/data/usecases/common"
	"github.com/timescale/tsbs/pkg/targets"
	generic "github.com/timescale/tsbs/pkg/targets/common"
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

type field struct {
	dataType arrow.DataType
	name     string
	value    string
}

type point struct {
	measurement string
	timestamp   string
	fields      []field
}

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
	// Scans the next line.
	ok := ds.scanner.Scan()
	if !ok {
		if err := ds.scanner.Err(); err != nil {
			log.Panicf("scan error: %v", err)
		}
		// Returns an empty data point so that the caller would know what happened.
		return data.LoadedPoint{}
	}

	// The serialized data point conforms to the following format:
	// <measurement> <timestamp> <field name>=<field value> <field name>=<field value> ... <compressed data types>
	// where the compressed data types conforms to the following format:
	// 0,1,2,3,4,...
	// Each integer corresponds to a variant of the DataType
	// and the number of integers is identical with the number of field values in the line.
	line := strings.TrimSpace(ds.scanner.Text())
	tokens := strings.Split(line, " ")
	point, err := decodePoint(tokens)
	if err != nil {
		log.Panicf("failed to decode point. error: %v", err)
	}
	return data.LoadedPoint{Data: point}
}

func decodePoint(tokens []string) (point, error) {
	measurement := tokens[0]
	timestamp := tokens[1]
	compressedDataTypes := tokens[len(tokens)-1]

	dataTypes := strings.Split(compressedDataTypes, ",")
	fields := make([]field, 0, len(dataTypes))
	for i, s := range dataTypes {
		dataType := arrowDataTypeFromString(s)

		keyValue := tokens[2+i]
		parts := strings.Split(keyValue, "=")
		name := parts[0]
		value := parts[1]

		fields = append(fields, field{dataType, name, value})
	}

	return point{measurement, timestamp, fields}, nil
}

// Gets the headers of the data source. Not used by Datalayers.
func (ds *dataSource) Headers() *common.GeneratedDataHeaders {
	return nil
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to.
type pointIndexer struct {
	inner *generic.GenericPointIndexer
}

// Creates a new point indexer.
func NewPointIndexer(maxPartitions uint) targets.PointIndexer {
	inner := generic.NewGenericPointIndexer(maxPartitions, selectPartition)
	return &pointIndexer{inner}
}

// hashPropertySelectFn defines a function that
// for a data.LoadedPoint return a byte array generated
// from the point properties that will be
// used to calculate the hash
//
// Selects the partition according to the hash of the measurement of each point.
func selectPartition(loadedPoint *data.LoadedPoint) []byte {
	point := loadedPoint.Data.(point)
	return []byte(point.measurement)
}

// GetIndex returns a partition for the given Point
func (indexer *pointIndexer) GetIndex(point data.LoadedPoint) uint {
	return indexer.inner.GetIndex(point)
}

// Batch is an aggregate of points for a particular data system.
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type batch struct {
	points []point
}

// Gets the current length of the batch.
// For Datalayers, the length is the number of data points currently stored in the batch.
func (b *batch) Len() uint {
	return uint(len(b.points))
}

// Appends a data point to the batch.
func (b *batch) Append(loadedPoint data.LoadedPoint) {
	point := loadedPoint.Data.(point)
	b.points = append(b.points, point)
}

// BatchFactory returns a new empty batch for storing points.
type batchFactory struct {
	capacity uint
}

// Creates a new batch factory.
func NewBatchFactory(capacity uint) targets.BatchFactory {
	return &batchFactory{capacity}
}

// New returns a new Batch to add Points to
func (bf *batchFactory) New() targets.Batch {
	return &batch{points: make([]point, 0, bf.capacity)}
}

func arrowDataTypeFromString(s string) arrow.DataType {
	i, err := strconv.Atoi(s)
	if err != nil {
		panic(fmt.Sprintf("failed to convert a string to arrow data type. error: %v", err))
	}

	tp := arrow.Type(i)
	switch tp {
	case arrow.NULL:
		return arrow.Null
	case arrow.BOOL:
		return arrow.FixedWidthTypes.Boolean
	case arrow.INT32:
		return arrow.PrimitiveTypes.Int32
	case arrow.INT64:
		return arrow.PrimitiveTypes.Int64
	case arrow.FLOAT32:
		return arrow.PrimitiveTypes.Float32
	case arrow.FLOAT64:
		return arrow.PrimitiveTypes.Float64
	case arrow.BINARY:
		return arrow.BinaryTypes.Binary
	case arrow.STRING:
		return arrow.BinaryTypes.String
	default:
		panic(fmt.Sprintf("unexpected arrow type: %v", tp))
	}
}
