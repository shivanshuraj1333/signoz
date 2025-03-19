# Binary name
BINARY_NAME=rca-agent
CLIENT_BINARY_NAME=rca-client

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean

# Build directory
BUILD_DIR=build

# Default webhook URL
DEFAULT_WEBHOOK_URL=http://localhost:8080/webhook

.PHONY: all build clean run start debug-alert send-alert setup-db clean-db test-debug test-webhook

# Default target
all: build

# Build the application
build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) .
	$(GOBUILD) -o $(BUILD_DIR)/$(CLIENT_BINARY_NAME) ./client

# Clean build artifacts
clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

# Run the application
run:
	$(GORUN) .

# Start the server
start: build
	./$(BUILD_DIR)/$(BINARY_NAME)

# Debug alert parsing
debug-alert:
	@if [ "$(file)" = "" ]; then \
		echo "Error: --file is required"; \
		exit 1; \
	fi
	@if [ ! -f "$(file)" ]; then \
		echo "Error: File $(file) does not exist"; \
		exit 1; \
	fi
	$(GORUN) . --debug --alert-file=$(file)

# Send alert to webhook
send-alert:
	@if [ -z "$(file)" ]; then \
		echo "Error: file parameter is required. Usage: make send-alert file='alert.json' [url='http://localhost:8080/webhook']"; \
		exit 1; \
	fi
	@if [ ! -f "$(file)" ]; then \
		echo "Error: File '$(file)' does not exist"; \
		exit 1; \
	fi
	@WEBHOOK_URL=$(if $(url),$(url),$(DEFAULT_WEBHOOK_URL)); \
	curl -X POST -H "Content-Type: application/json" -d @$(file) $$WEBHOOK_URL

# Setup database
setup-db:
	@chmod +x scripts/setup-db.sh
	@./scripts/setup-db.sh

# Clean database
clean-db:
	@if [ ! -f .env ]; then \
		echo "Error: .env file not found"; \
		exit 1; \
	fi
	@source .env && \
	echo "Dropping database..." && \
	PGPASSWORD=$$DB_PASSWORD psql "postgresql://$$DB_USER@$$DB_HOST:$$DB_PORT/postgres?sslmode=require" -c "DROP DATABASE IF EXISTS $$DB_NAME;" || true

# Test debug mode with sample alert
test-debug:
	$(GORUN) . --debug --alert-file=testdata/alert.json

# Test webhook with sample alert
test-webhook:
	curl -X POST http://localhost:8080/webhook \
		-H "Content-Type: application/json" \
		-d @testdata/alert.json 