package datalayers

import (
	"github.com/timescale/tsbs/pkg/data"
	"github.com/timescale/tsbs/pkg/targets"
)

// Batch is an aggregate of points for a particular data system.
// It needs to have a way to measure it's size to make sure
// it does not get too large and it needs a way to append a point
type batch struct {
}

func (b *batch) Len() uint {
	panic("")
}

func (b *batch) Append(point data.LoadedPoint) uint {
	panic("")
}

// BatchFactory returns a new empty batch for storing points.
type batchFactory struct {
}

// New returns a new Batch to add Points to
func (bf *batchFactory) New() targets.Batch {
	panic("")
}

// PointIndexer determines the index of the Batch (and subsequently the channel)
// that a particular point belongs to
type pointIndexer struct {
}

// GetIndex returns a partition for the given Point
func (indexer *pointIndexer) GetIndex(point data.LoadedPoint) uint {
	panic("")
}
