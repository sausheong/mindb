.PHONY: build test run clean docker help

# Binary name
BINARY=mindb-server
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

build: ## Build the binary
	@echo "Building $(BINARY)..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY) .
	@echo "Build complete: $(BUILD_DIR)/$(BINARY)"

test: ## Run tests
	@echo "Running tests..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	@echo "Tests complete"

test-coverage: test ## Run tests with coverage report
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

run: ## Run the server
	@echo "Running $(BINARY)..."
	$(GORUN) .

clean: ## Clean build artifacts
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	@echo "Clean complete"

deps: ## Download dependencies
	@echo "Downloading dependencies..."
	$(GOMOD) download
	$(GOMOD) tidy
	@echo "Dependencies updated"

docker: ## Build Docker image
	@echo "Building Docker image..."
	docker build -t $(BINARY):latest -f Dockerfile ../..
	@echo "Docker image built: $(BINARY):latest"

docker-run: docker ## Run Docker container
	docker run --rm -p 8080:8080 \
		-e MINDB_DATA_DIR=/data \
		-e AUTH_DISABLED=true \
		-v $(PWD)/data:/data \
		$(BINARY):latest

lint: ## Run linter
	@echo "Running linter..."
	golangci-lint run ./...

fmt: ## Format code
	@echo "Formatting code..."
	$(GOCMD) fmt ./...
	goimports -w .

install: build ## Install binary to $GOPATH/bin
	@echo "Installing $(BINARY)..."
	cp $(BUILD_DIR)/$(BINARY) $(GOPATH)/bin/
	@echo "Installed to $(GOPATH)/bin/$(BINARY)"

.DEFAULT_GOAL := help
