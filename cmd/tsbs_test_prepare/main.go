package main

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"github.com/prometheus/common/log"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const USE_EXECUTE_UPDATE bool = false

func execute(client *flightsql.Client, ctx context.Context, query string) {
	flightInfo, err := client.Execute(ctx, query)
	if err != nil {
		panic(err)
	}

	flightReader, err := client.DoGet(ctx, flightInfo.GetEndpoint()[0].GetTicket())
	if err != nil {
		if strings.Contains(err.Error(), "already exists") {
			return
		}
		panic(err)
	}
	flightReader.Release()
}

func run() error {
	sqlEndpoint := "127.0.0.1:28360"
	dbName := "benchmark"
	tableName := "cpu"

	var grpcDialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	client, err := flightsql.NewClient(sqlEndpoint, nil, nil, grpcDialOpts...)
	if err != nil {
		panic(err)
	}

	// Handshakes.
	ctx, err := client.Client.AuthenticateBasicToken(context.Background(), "admin", "public")
	if err != nil {
		panic(err)
	}

	// Set request metadata to include the database context.
	ctx = metadata.AppendToOutgoingContext(ctx, "database", dbName)

	// Create database.
	createDatabaseStmt := fmt.Sprintf("CREATE DATABASE %s", dbName)
	execute(client, ctx, createDatabaseStmt)

	// Create table.
	createClause := fmt.Sprintf("CREATE TABLE IF NOT EXISTS %v", tableName)
	columnDefs := strings.Join([]string{
		"ts TIMESTAMP(9) NOT NULL DEFAULT CURRENT_TIMESTAMP",
		"hostname INT32",
		"region INT32",
		"timestamp key(ts)",
	}, ",\n")
	columnDefClause := fmt.Sprintf("(\n%v\n)", columnDefs)
	partitionByClause := "PARTITION BY HASH(hostname) PARTITIONS 8"
	tableOptions := "ENGINE=TimeSeries"

	createTableStmt := strings.Join([]string{createClause, columnDefClause, partitionByClause, tableOptions}, "\n")
	log.Infof("create table stmt:\n%v", createTableStmt)

	execute(client, ctx, createTableStmt)

	// Create a insert prepared statement.
	query := fmt.Sprintf("INSERT INTO %v (ts,hostname,region) VALUES (?,?,?)", tableName)
	preparedStmt, err := client.Prepare(ctx, query)
	if err != nil {
		panic(err)
	}

	// Set a record builder for building arrow records.
	arrowFields := make([]arrow.Field, 0, 3)
	arrowFields = append(arrowFields, arrow.Field{
		Name:     "ts",
		Type:     arrow.FixedWidthTypes.Timestamp_ns,
		Nullable: false,
	})
	arrowFields = append(arrowFields, arrow.Field{
		Name:     "hostname",
		Type:     arrow.PrimitiveTypes.Int32,
		Nullable: true,
	})
	arrowFields = append(arrowFields, arrow.Field{
		Name:     "region",
		Type:     arrow.PrimitiveTypes.Int32,
		Nullable: true,
	})
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	arrowRecordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)

	// Inserts data with the prepared statement.
	for iter := 0; iter < 10; iter++ {
		// Builds a record.
		for i, field := range arrowFields {
			fieldBuilder := arrowRecordBuilder.Field(i)
			fieldValue := ""
			switch field.Type {
			case arrow.PrimitiveTypes.Int32:
				fieldValue = strconv.Itoa(iter + i)
			case arrow.FixedWidthTypes.Timestamp_ns:
				fieldValue = strconv.Itoa(time.Now().UTC().Nanosecond())
			}
			appendFieldValue(fieldBuilder, field.Type, fieldValue)
		}
		record := arrowRecordBuilder.NewRecord()

		// Binds the record to the prepared statment.
		// This binding only affects the client-side binding. No data will be transmitted to the server.
		preparedStmt.SetParameters(record)

		// There're two ways to execute the prepared statement.
		if USE_EXECUTE_UPDATE {
			// 1. Use the ExecuteUpdate method to execute the prepared statement as an update query, i.e. create, insert, delete, etc.
			// The server returns the number of affected rows.
			affectedRows, err := preparedStmt.ExecuteUpdate(ctx)
			if err != nil {
				panic(err)
			}
			log.Infof("affected rows = %v", affectedRows)
		} else {
			// 2. Use the Execute method to execute the prepared statement as a general query.
			// The server returns a flight into through which the client could fetch the execution result by calling DoGet.
			flightInfo, err := preparedStmt.Execute(ctx)
			if err != nil {
				panic(err)
			}

			flightReader, err := client.DoGet(ctx, flightInfo.GetEndpoint()[0].GetTicket())
			if err != nil {
				panic(err)
			}
			flightReader.Release()

			log.Infof("inserted a record")
		}

		record.Release()
	}

	return nil
}

func appendFieldValue(fieldBuilder array.Builder, fieldType arrow.DataType, fieldValue string) {
	switch fieldType {
	case arrow.FixedWidthTypes.Boolean:
		builder := fieldBuilder.(*array.BooleanBuilder)
		v := fieldValue == "true"
		builder.Append(v)
	case arrow.PrimitiveTypes.Int32:
		builder := fieldBuilder.(*array.Int32Builder)
		v, err := strconv.ParseInt(fieldValue, 10, 32)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to int32. error: %v", err))
		}
		builder.Append(int32(v))
	case arrow.PrimitiveTypes.Int64:
		builder := fieldBuilder.(*array.Int64Builder)
		v, err := strconv.ParseInt(fieldValue, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to int64. error: %v", err))
		}
		builder.Append(v)
	case arrow.PrimitiveTypes.Float32:
		builder := fieldBuilder.(*array.Float32Builder)
		v, err := strconv.ParseFloat(fieldValue, 32)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to float32. error: %v", err))
		}
		builder.Append(float32(v))
	case arrow.PrimitiveTypes.Float64:
		builder := fieldBuilder.(*array.Float64Builder)
		v, err := strconv.ParseFloat(fieldValue, 64)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to float64. error: %v", err))
		}
		builder.Append(v)
	case arrow.BinaryTypes.Binary:
		builder := fieldBuilder.(*array.BinaryBuilder)
		v := []byte(fieldValue)
		builder.Append(v)
	case arrow.BinaryTypes.String:
		builder := fieldBuilder.(*array.StringBuilder)
		v := fieldValue
		builder.Append(v)
	case arrow.FixedWidthTypes.Timestamp_ns:
		builder := fieldBuilder.(*array.TimestampBuilder)
		ts, err := strconv.ParseInt(fieldValue, 10, 64)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to int64. error: %v", err))
		}
		v, err := arrow.TimestampFromTime(time.Unix(0, ts).UTC(), arrow.Nanosecond)
		if err != nil {
			panic(fmt.Sprintf("failed to convert a string to timestamp. error: %v", err))
		}
		builder.Append(v)
	default:
		panic(fmt.Sprintf("unexpected field type: %v", fieldType))
	}
}

func main() {
	run()
}
