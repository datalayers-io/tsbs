package query

import (
	"fmt"
	"sync"
)

// A encoded Arrow Flight SQL query with some metadata attached.
type FlightSqlQuery struct {
	HumanLabel       []byte
	HumanDescription []byte
	RawQuery         []byte
	id               uint64
}

// A pool for saving and retriving encoded Arrow Flight SQL queries.
var FlightSqlQueryPool = sync.Pool{
	New: func() interface{} {
		return &FlightSqlQuery{
			HumanLabel:       []byte{},
			HumanDescription: []byte{},
			RawQuery:         []byte{},
		}
	},
}

// NewFlightSqlQuery returns a new FlightSqlQuery type Query
func NewFlightSqlQuery() *FlightSqlQuery {
	return FlightSqlQueryPool.Get().(*FlightSqlQuery)
}

// GetID returns the ID of this Query
// Warning: GetID is not used for now.
func (q *FlightSqlQuery) GetID() uint64 {
	return q.id
}

// SetID sets the ID for this Query
func (q *FlightSqlQuery) SetID(n uint64) {
	q.id = n
}

// String produces a debug-ready description of a Query.
func (q *FlightSqlQuery) String() string {
	return fmt.Sprintf("HumanLabel: \"%s\", HumanDescription: \"%s\"", q.HumanLabel, q.HumanDescription)
}

// HumanLabelName returns the human readable name of this Query
func (q *FlightSqlQuery) HumanLabelName() []byte {
	return q.HumanLabel
}

// HumanDescriptionName returns the human readable description of this Query
func (q *FlightSqlQuery) HumanDescriptionName() []byte {
	return q.HumanDescription
}

// Release resets and returns this Query to its pool
func (q *FlightSqlQuery) Release() {
	q.HumanLabel = q.HumanLabel[:0]
	q.HumanDescription = q.HumanDescription[:0]
	q.id = 0

	FlightSqlQueryPool.Put(q)
}
