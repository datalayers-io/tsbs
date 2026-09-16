# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

.PHONY: all generators loaders runners tools lint fmt checkfmt

# Only the Datalayers benchmark pipeline binaries are built here.
# Influx/TimescaleDB loaders/runners and the other Datalayers helper tools
# (cmd/dump_dl_query etc.) are intentionally excluded; build them manually.
all: generators loaders runners tools

generators: tsbs_generate_data \
			tsbs_generate_queries

loaders: tsbs_load

runners: tsbs_run_queries_datalayers

tools: rewrite_query_hints_config

rewrite_query_hints_config:
	$(GOGET) ./cmd/rewrite_query_hints_config
	$(GOBUILD) -o bin/rewrite_query_hints_config ./cmd/rewrite_query_hints_config
	$(GOINSTALL) ./cmd/rewrite_query_hints_config

test:
	$(GOTEST) -v ./...

coverage:
	$(GOTEST) -race -coverprofile=coverage.txt -covermode=atomic ./...

# Release mode.
tsbs_%: $(wildcard ./cmd/$@/*.go)
	$(GOGET) ./cmd/$@
	$(GOBUILD) -o bin/$@ ./cmd/$@
	$(GOINSTALL) ./cmd/$@

# Debug mode.
# tsbs_%: $(wildcard ./cmd/$@/*.go)
# 	$(GOGET) ./cmd/$@
# 	$(GOBUILD) -gcflags "all=-N -l" -o bin/$@ ./cmd/$@
# 	$(GOINSTALL) ./cmd/$@

checkfmt:
	@echo 'Checking gofmt';\
 	bash -c "diff -u <(echo -n) <(gofmt -d .)";\
	EXIT_CODE=$$?;\
	if [ "$$EXIT_CODE"  -ne 0 ]; then \
		echo '$@: Go files must be formatted with gofmt'; \
	fi && \
	exit $$EXIT_CODE

lint:
	$(GOGET) github.com/golangci/golangci-lint/cmd/golangci-lint
	golangci-lint run

fmt:
	$(GOFMT) ./...
