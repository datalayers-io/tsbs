package datalayers

import (
	"context"
	"fmt"
	"strings"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

// A Datalayers client based on an Arrow FlightSql client.
type Client struct {
	inner *flightsql.Client
	ctx   context.Context
}

// Creates a Datalayers client to connect to the given socket address.
func NewClient(sqlEndpoint string) (*Client, error) {
	// Creates a flight sql client.
	var grpcDialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	flightSqlClient, err := flightsql.NewClient(sqlEndpoint, nil, nil, grpcDialOpts...)
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

func (clt *Client) Close() error {
	return clt.inner.Close()
}

func (clt *Client) CreateDatabase(dbName string) error {
	createDatabaseStmt := fmt.Sprintf("create database %s", dbName)
	return clt.GeneralExecute(createDatabaseStmt)
}

func (clt *Client) UseDatabase(dbName string) {
	clt.ctx = metadata.AppendToOutgoingContext(clt.ctx, "database", dbName)
}

func (clt *Client) CreateTable(dbName string, tableName string, ifNotExists bool, arrowFields []arrow.Field, partitionByFields []string, partitionNum uint) error {
	createClause := "CREATE TABLE "
	if ifNotExists {
		createClause += "IF NOT EXISTS "
	}
	createClause += fmt.Sprintf("%v.%v", dbName, tableName)

	columnDefs := make([]string, 0, len(arrowFields))
	for _, field := range arrowFields {
		colDef := fmt.Sprintf("%v %v", field.Name, arrowDataTypeToDatalayersDataType(field.Type))
		// Adds a column constraint if it's the timestamp field.
		if field.Name == "ts" {
			colDef += " NOT NULL DEFAULT CURRENT_TIMESTAMP"
		}
		columnDefs = append(columnDefs, colDef)
	}
	// Adds the timestamp constraint.
	columnDefs = append(columnDefs, "timestamp key(ts)")
	columnDefClause := fmt.Sprintf("(\n%v\n)", strings.Join(columnDefs, ",\n"))

	partitionByClause := fmt.Sprintf("PARTITION BY HASH(%v) PARTITIONS %v", strings.Join(partitionByFields, ","), partitionNum)
	engineClause := "ENGINE=TimeSeries"

	allClauses := []string{createClause, columnDefClause, partitionByClause, engineClause}
	createTableStmt := strings.Join(allClauses, "\n")

	log.Debugf("The create table statement for table %v is:\n%v", tableName, createTableStmt)

	return clt.GeneralExecute(createTableStmt)
}

func (clt *Client) InsertPrepare(dbName string, tableName string, arrowFields []arrow.Field) (*flightsql.PreparedStatement, error) {
	fieldNames := make([]string, 0, len(arrowFields))
	placeHolders := make([]string, 0, len(arrowFields))

	for _, field := range arrowFields {
		fieldNames = append(fieldNames, field.Name)
		placeHolders = append(placeHolders, "?")
	}

	insertPrepareStmt := fmt.Sprintf("INSERT INTO %v.%v (%v) VALUES (%v)", dbName, tableName, strings.Join(fieldNames, ","), strings.Join(placeHolders, ","))

	log.Debugf("The prepared statement for inserting into table %v is:\n%v", tableName, insertPrepareStmt)

	return clt.inner.Prepare(clt.ctx, insertPrepareStmt)
}

func (clt *Client) ExecuteInsertPrepare(preparedStatement *flightsql.PreparedStatement) error {
	flightInfo, err := preparedStatement.Execute(clt.ctx)
	if err != nil {
		return err
	}
	return clt.doGetWithFlightInfo(flightInfo)
	// affectedRows, err := preparedStatement.ExecuteUpdate(clt.ctx)
	// if err != nil {
	// 	return err
	// }
	// log.Infof("Insert prepared affected rows: %v", affectedRows)
	// return nil
}

func (clt *Client) GeneralExecute(query string) error {
	flightInfo, err := clt.inner.Execute(clt.ctx, query)
	if err != nil {
		return err
	}
	return clt.doGetWithFlightInfo(flightInfo)
}

func (clt *Client) doGetWithFlightInfo(flightInfo *flight.FlightInfo) error {
	// Assumes the server is in the standalone mode.
	ticket := flightInfo.GetEndpoint()[0].GetTicket()
	flightReader, err := clt.inner.DoGet(clt.ctx, ticket)
	if err != nil {
		return err
	}

	flightReader.Release()
	return nil
}

// TODO(niebayes): support pretty print response.
func (clt *Client) ExecuteQuery(query string) error {
	log.Infof("Execute Query: %v", query)

	flightInfo, err := clt.inner.Execute(clt.ctx, query)
	if err != nil {
		panic(err)
	}

	flightReader, err := clt.inner.DoGet(clt.ctx, flightInfo.GetEndpoint()[0].GetTicket())
	if err != nil {
		panic(err)
	}
	for flightReader.Next() {
		record := flightReader.Record()
		log.Infof("Read a record. numRows: %v, numCols: %v", record.NumRows(), record.NumCols())
	}
	flightReader.Release()

	return nil
}

func arrowDataTypeToDatalayersDataType(arrowDataType arrow.DataType) string {
	switch arrowDataType {
	case arrow.FixedWidthTypes.Boolean:
		return "BOOLEAN"
	case arrow.PrimitiveTypes.Int32:
		return "INT32"
	case arrow.PrimitiveTypes.Int64:
		return "INT64"
	case arrow.PrimitiveTypes.Float32:
		return "REAL"
	case arrow.PrimitiveTypes.Float64:
		return "DOUBLE"
	case arrow.BinaryTypes.Binary:
		return "BINARY"
	case arrow.BinaryTypes.String:
		return "STRING"
	case arrow.FixedWidthTypes.Timestamp_ns:
		return "TIMESTAMP(9)"
	default:
		panic(fmt.Sprintf("unexpected arrow data type %v", arrowDataType))
	}
}
