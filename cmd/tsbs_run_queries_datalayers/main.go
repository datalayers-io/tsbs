package main

import (
	"fmt"

	"github.com/blagojts/viper"
	"github.com/spf13/pflag"
	"github.com/timescale/tsbs/internal/utils"
	"github.com/timescale/tsbs/pkg/query"
)

var (
	// The Arrow Flight SQL endpoint exposed by the Datalayers server.
	sqlEndpoint string
	// The runner for running query benchmarks.
	runner *query.BenchmarkRunner
)

func addDatalayersSpecificFlags() {
	pflag.String("sql-endpoint", "127.0.0.1:8360", "The Arrow Flight SQL endpoint exposed by the Datalayers server")
}

func init() {
	// Parse command line args and setup configurations.
	var config query.BenchmarkRunnerConfig
	config.AddToFlagSet(pflag.CommandLine)
	addDatalayersSpecificFlags()
	pflag.Parse()

	err := utils.SetupConfigFile()
	if err != nil {
		panic(fmt.Errorf("fatal error config file: %s", err))
	}
	if err = viper.Unmarshal(&config); err != nil {
		panic(fmt.Errorf("unable to decode config: %s", err))
	}

	// Set the `sqlEndpoint` global variable.
	sqlEndpoint = viper.GetString("sql-endpoint")
	if len(sqlEndpoint) == 0 {
		panic("missing sql endpoint")
	}

	// Initialize the runner.
	runner = query.NewBenchmarkRunner(config)
}

func main() {
	runner.Run(&query.FlightSqlQueryPool, newProcessor)
}
