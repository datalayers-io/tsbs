package datalayers

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// A Datalayers client based on an Arrow FlightSql client.
type Client struct {
	inner *flightsql.Client
	ctx   context.Context
}

// Creates a Datalayers client to connect to the given socket address.
func NewClient(addr string) (*Client, error) {
	// Creates a flight sql client.
	var grpcDialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	flightSqlClient, err := flightsql.NewClient(addr, nil, nil, grpcDialOpts...)
	if err != nil {
		return nil, err
	}

	// Handshakes.
	ctx, err := flightSqlClient.Client.AuthenticateBasicToken(context.Background(), "admin", "public")
	if err != nil {
		return nil, err
	}

	clt := &Client{flightSqlClient, ctx}
	return clt, nil
}

// Execute executes the desired query on the server and returns a FlightInfo
// object describing where to retrieve the results.
func (clt *Client) Execute(query string) (*flight.FlightInfo, error) {
	return clt.inner.Execute(clt.ctx, query)
}

// DoGet uses the provided flight ticket to request the stream of data.
// It returns a recordbatch reader to stream the results. Release
// should be called on the reader when done.
func (clt *Client) DoGet(ticket *flight.Ticket) (*flight.Reader, error) {
	return clt.inner.DoGet(clt.ctx, ticket)
}

// Prepare creates a PreparedStatement object for the specified query.
// The resulting PreparedStatement object should be Closed when no longer
// needed. It will maintain a reference to this Client for use to execute
// and use the specified allocator for any allocations it needs to perform.
func (clt *Client) Prepare(query string) (*flightsql.PreparedStatement, error) {
	return clt.inner.Prepare(clt.ctx, query)
}

func (clt *Client) CreateDatabase(dbName string) error {
	create_database_stmt := fmt.Sprintf("create database %s", dbName)
	flightInfo, err := clt.Execute(create_database_stmt)
	if err != nil {
		return err
	}

	// Assumes the server is in the standalone mode.
	ticket := flightInfo.GetEndpoint()[0].GetTicket()

	// Ignores the reader since we only care about whether the database was created successfully or not.
	_, err = clt.DoGet(ticket)
	return err
}
