# Go parameters
GOCMD=GO111MODULE=on go
GOBUILD=$(GOCMD) build
GOINSTALL=$(GOCMD) install
GOCLEAN=$(GOCMD) clean
GOTEST=$(GOCMD) test
GOGET=$(GOCMD) get
GOMOD=$(GOCMD) mod
GOFMT=$(GOCMD) fmt

.PHONY: all generators loaders runners lint fmt checkfmt

all: generators loaders runners

generators: tsbs_generate_data \
			tsbs_generate_queries

loaders: tsbs_load
# loaders: tsbs_load \
# 		 tsbs_load_influx \
# 		 tsbs_load_timescaledb

runners: tsbs_run_queries_datalayers
# runners: tsbs_run_queries_influx \
# 		 tsbs_run_queries_timescaledb \
# 		 tsbs_run_queries_datalayers

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
