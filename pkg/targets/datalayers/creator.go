package datalayers

// DBCreator is an interface for a benchmark to do the initial setup of a database
// in preparation for running a benchmark against it.
//
// Datalayers' implementation of the DBCreator interface.
type dBCreator struct {
}

// Init should set up any connection or other setup for talking to the DB, but should NOT create any databases
func (dc *dBCreator) Init() {
	panic("")
}

// DBExists checks if a database with the given name currently exists.
func (dc *dBCreator) DBExists(dbName string) bool {
	// Not implemented.
	return false
}

// CreateDB creates a database with the given name.
func (dc *dBCreator) CreateDB(dbName string) error {
	panic("")
}

// RemoveOldDB removes an existing database with the given name.
func (dc *dBCreator) RemoveOldDB(dbName string) error {
	// Not implemented.
	return nil
}

// DBCreatorCloser is a DBCreator that also needs a Close method to cleanup any connections
// after the benchmark is finished.
//
// Close cleans up any database connections. Only needed by the DBCreatorCloser interface.
func (dc *dBCreator) Close() {
	// Not implemented.
}

// DBCreatorPost is a DBCreator that also needs to do some initialization after the
// database is created (e.g., only one client should actually create the DB, so
// non-creator clients should still set themselves up for writing)
//
// PostCreateDB does further initialization after the database is created
func (dc *dBCreator) PostCreateDB(dbName string) error {
	// Not implemented.
	return nil
}
