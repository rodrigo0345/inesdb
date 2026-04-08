.PHONY: help build run test clean setup lint fmt

# Build variables
BINARY_NAME=omag
CMD_PATH=./cmd/cli
GO=go
GOFLAGS=-v
VERSION?=$(shell git describe --tags --always --dirty)
BUILD_DATE=$(shell date -u '+%Y-%m-%d_%H:%M:%S')

help: ## Display this help screen
	@echo "Usage: make [target]"
	@echo ""
	@echo "Targets:"
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "  %-20s %s\n", $$1, $$2}'

setup: ## Set up development environment
	@echo "📦 Setting up development environment..."
	@$(GO) mod download
	@$(GO) mod tidy
	@echo "✅ Setup complete"

build: ## Build the application
	@echo "🔨 Building $(BINARY_NAME)..."
	@$(GO) build $(GOFLAGS) \
		-ldflags="-X main.Version=$(VERSION) -X main.BuildDate=$(BUILD_DATE)" \
		-o bin/$(BINARY_NAME) $(CMD_PATH)
	@echo "✅ Build complete: bin/$(BINARY_NAME)"

run: build ## Build and run the application
	@echo "▶️  Running $(BINARY_NAME)..."
	@./bin/$(BINARY_NAME)

test: ## Run all tests
	@echo "🧪 Running tests..."
	@$(GO) test -v ./... -cover
	@echo "✅ Tests complete"

test-coverage: ## Run tests with coverage report
	@echo "📊 Running tests with coverage..."
	@$(GO) test -v ./... -coverprofile=coverage.out -covermode=atomic
	@$(GO) tool cover -html=coverage.out -o coverage.html
	@echo "✅ Coverage report: coverage.html"

test-short: ## Run tests without long-running tests
	@echo "⚡ Running short tests..."
	@$(GO) test -v ./... -short

lint: ## Run linter
	@echo "🔍 Linting..."
	@which golangci-lint > /dev/null || (echo "Installing golangci-lint..." && go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest)
	@golangci-lint run ./...
	@echo "✅ Lint complete"

fmt: ## Format code
	@echo "🎨 Formatting code..."
	@$(GO) fmt ./...
	@goimports -w .
	@echo "✅ Format complete"

vet: ## Run go vet
	@echo "🔧 Running go vet..."
	@$(GO) vet ./...
	@echo "✅ Vet complete"

clean: ## Clean build artifacts
	@echo "🧹 Cleaning..."
	@rm -rf bin/
	@rm -rf coverage.out coverage.html
	@$(GO) clean
	@echo "✅ Clean complete"

mod-tidy: ## Tidy Go modules
	@echo "📋 Tidying modules..."
	@$(GO) mod tidy
	@echo "✅ Modules tidied"

mod-verify: ## Verify Go modules
	@echo "✔️  Verifying modules..."
	@$(GO) mod verify
	@echo "✅ Modules verified"

# Development quality assurance
qa: fmt vet lint test ## Run full QA suite (fmt, vet, lint, test)
	@echo "✅ QA complete!"

# Docker targets (optional)
docker-build: ## Build Docker image
	@echo "🐳 Building Docker image..."
	@docker build -t $(BINARY_NAME):$(VERSION) .
	@echo "✅ Docker image built"

docker-run: ## Run Docker container
	@echo "🐳 Running Docker container..."
	@docker run -it $(BINARY_NAME):$(VERSION)

# Project structure
structure: ## Show project structure
	@echo "📁 Project Structure:"
	@tree -I 'vendor|.git|bin|coverage*' -L 3

# Utility targets
info: ## Show build information
	@echo "Build Information:"
	@echo "  Binary: $(BINARY_NAME)"
	@echo "  Version: $(VERSION)"
	@echo "  Build Date: $(BUILD_DATE)"
	@echo "  Go Version: $(shell $(GO) version)"

deps: ## List dependencies
	@echo "📦 Dependencies:"
	@$(GO) list -m all

.DEFAULT_GOAL := help
