.PHONY: build test run clean docker help web-check

# Binary name
BINARY=mindb-server
BUILD_DIR=bin
WEB_DIR=web

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

web-check: ## Check if web console files exist
	@echo "Checking web console files..."
	@test -f $(WEB_DIR)/index.html || (echo "❌ Missing: $(WEB_DIR)/index.html" && exit 1)
	@test -f $(WEB_DIR)/styles.css || (echo "❌ Missing: $(WEB_DIR)/styles.css" && exit 1)
	@test -f $(WEB_DIR)/app.js || (echo "❌ Missing: $(WEB_DIR)/app.js" && exit 1)
	@echo "✅ Web console files present"

build: web-check ## Build the binary (includes web console check)
	@echo "Building $(BINARY) with integrated web console..."
	@mkdir -p $(BUILD_DIR)
	$(GOBUILD) $(LDFLAGS) -o $(BUILD_DIR)/$(BINARY) .
	@echo "✅ Build complete: $(BUILD_DIR)/$(BINARY)"
	@echo "📝 Note: Make sure to copy the 'web/' directory alongside the binary"

test: ## Run tests
	@echo "Running tests..."
	$(GOTEST) -v -race -coverprofile=coverage.out ./...
	@echo "Tests complete"

test-coverage: test ## Run tests with coverage report
	$(GOCMD) tool cover -html=coverage.out -o coverage.html
	@echo "Coverage report: coverage.html"

run: web-check ## Run the server with web console
	@echo "Starting $(BINARY) with integrated web console..."
	@echo "📝 API Server: http://localhost:8080"
	@echo "🌐 Web Console: http://localhost:8080/console"
	@echo ""
	$(GORUN) .

dev: ## Run in development mode with auto-reload (requires air)
	@echo "Starting development server..."
	@command -v air > /dev/null || (echo "❌ 'air' not found. Install with: go install github.com/cosmtrek/air@latest" && exit 1)
	air

clean: ## Clean build artifacts
	@echo "Cleaning..."
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)
	rm -f coverage.out coverage.html
	@echo "Clean complete"

package: build ## Package binary with web console
	@echo "Packaging $(BINARY) with web console..."
	@mkdir -p $(BUILD_DIR)/package
	@cp $(BUILD_DIR)/$(BINARY) $(BUILD_DIR)/package/
	@cp -r $(WEB_DIR) $(BUILD_DIR)/package/
	@cp README.md $(BUILD_DIR)/package/ 2>/dev/null || true
	@echo "✅ Package created: $(BUILD_DIR)/package/"
	@echo "📦 Contents:"
	@ls -lh $(BUILD_DIR)/package/

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
	@mkdir -p $(GOPATH)/bin/mindb-server-data
	@cp $(BUILD_DIR)/$(BINARY) $(GOPATH)/bin/
	@cp -r $(WEB_DIR) $(GOPATH)/bin/mindb-server-data/
	@echo "✅ Installed to $(GOPATH)/bin/$(BINARY)"
	@echo "📁 Web console copied to $(GOPATH)/bin/mindb-server-data/web/"
	@echo "⚠️  Run from $(GOPATH)/bin/mindb-server-data/ to serve web console"

web-info: ## Show web console information
	@echo "╔════════════════════════════════════════════════════════════╗"
	@echo "║          Mindb Server - Web Console Integration           ║"
	@echo "╚════════════════════════════════════════════════════════════╝"
	@echo ""
	@echo "📁 Web Console Location: ./$(WEB_DIR)/"
	@echo "🌐 Access URL: http://localhost:8080/console"
	@echo ""
	@echo "Files:"
	@ls -lh $(WEB_DIR)/ | tail -n +2
	@echo ""
	@echo "Usage:"
	@echo "  make run      - Start server with web console"
	@echo "  make build    - Build binary (remember to copy web/ folder)"
	@echo "  make package  - Create deployable package with web console"
	@echo ""

.DEFAULT_GOAL := help
