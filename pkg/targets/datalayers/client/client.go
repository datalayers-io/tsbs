package client

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type datalayersClient struct {
}

func main() {
	addr := "127.0.0.1:8360"
	var dialOpts = []grpc.DialOption{
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	}
	cl, err := flightsql.NewClient(addr, nil, nil, dialOpts...)
	if err != nil {
		fmt.Println(err)
		return
	}
	ctx, err := cl.Client.AuthenticateBasicToken(context.Background(), "admin", "public")
	if err != nil {
		fmt.Print(err)
		return
	}
}
