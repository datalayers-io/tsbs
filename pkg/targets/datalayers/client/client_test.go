package datalayers

import (
	"testing"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

// Tests that the Datalayers' serializer works as expected.
func TestSelectPrepare(t *testing.T) {
	client, err := NewClient("127.0.0.1:28360")
	client.UseDatabase("benchmark")
	if err != nil {
		panic(err)
	}
	if err = SelectPrepare(client); err != nil {
		panic(err)
	}
}

func SelectPrepare(clt *Client) error {
	query := `SELECT date_trunc('minute', ts) AS minute, 
        max(usage_user)
        FROM cpu
        WHERE hostname IN (?) 
		AND ts >= '2016-01-01T03:52:45Z' AND ts < '2016-01-01T04:52:45Z'
        GROUP BY minute 
		ORDER BY minute ASC`
	preparedStatement, err := clt.inner.Prepare(clt.ctx, query)
	if err != nil {
		panic(err)
	}

	arrowFields := []arrow.Field{
		{
			Name:     "hostname",
			Type:     arrow.BinaryTypes.String,
			Nullable: false,
		},
	}
	arrowSchema := arrow.NewSchema(arrowFields, nil)
	arrowRecordBuilder := array.NewRecordBuilder(memory.NewGoAllocator(), arrowSchema)

	builder := arrowRecordBuilder.Field(0).(*array.StringBuilder)
	builder.Append("host_2")

	record := arrowRecordBuilder.NewRecord()
	preparedStatement.SetParameters(record)
	flightInfo, err := preparedStatement.Execute(clt.ctx)
	if err != nil {
		panic(err)
	}

	flightReader, err := clt.inner.DoGet(clt.ctx, flightInfo.GetEndpoint()[0].GetTicket())
	if err != nil {
		panic(err)
	}
	for flightReader.Next() {
		_ = flightReader.Record()
		println("Read a record")
	}
	flightReader.Release()

	record.Release()

	return nil
}
