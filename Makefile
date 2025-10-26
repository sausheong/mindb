.PHONY: build build-cli build-server test test-core test-server run-cli run-server dev clean deps fmt lint help

# Binary names
CLI_BINARY=mindb-cli
SERVER_BINARY=mindb
BUILD_DIR=bin

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GOTEST=$(GOCMD) test
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean
GOMOD=$(GOCMD) mod

# Build flags
LDFLAGS=-ldflags "-s -w"

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-15s\033[0m %s\n", $$1, $$2}'

build: build-cli build-server ## Build both CLI and server binaries

build-cli: ## Build the CLI binary
	@echo "Building $(CLI_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(CLI_BINARY) ./src/cli
	@echo "✅ CLI build complete: $(BUILD_DIR)/$(CLI_BINARY)"

build-server: ## Build the server binary
	@echo "Building $(SERVER_BINARY)..."
	@mkdir -p $(BUILD_DIR)
	cd src/server && $(GOBUILD) $(LDFLAGS) -o ../../$(BUILD_DIR)/$(SERVER_BINARY) .
	@echo "✅ Server build complete: $(BUILD_DIR)/$(SERVER_BINARY)"

test: test-core test-server ## Run all tests

test-core: ## Run core library tests
	@echo "Running core tests..."
	$(GOTEST) -v -race -coverprofile=coverage_core.out ./src/core/...
	@echo "Core tests complete"

test-server: ## Run server tests
	@echo "Running server tests..."
	cd src/server && $(GOTEST) -v -race -coverprofile=coverage_server.out ./...
	@echo "Server tests complete"

run-cli: ## Run the CLI client (connects to server at http://localhost:8080)
	@echo "Running CLI client..."
	$(GORUN) ./src/cli

run-server: ## Run the server
	@echo "Running server..."
	cd src/server && $(GORUN) .

dev: ## Run server and CLI together for development
	@echo "Starting development mode (server + CLI)..."
	@echo "Server will start in background, CLI will connect to it"
	@echo "Use Ctrl+C to stop both"
	@(cd src/server && $(GORUN) . &) && sleep 2 && $(GORUN) ./src/cli

clean: ## Clean build artifacts
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage_*.out coverage_*.html
	@echo "Clean complete"

deps: ## Download and tidy dependencies
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy
	@echo "Dependencies updated"

fmt: ## Format code
	@echo "Formatting code..."
	$(GOCMD) fmt ./...
	goimports -w .

lint: ## Run linter
	@echo "Running linter..."
	golangci-lint run ./...

.DEFAULT_GOAL := help
