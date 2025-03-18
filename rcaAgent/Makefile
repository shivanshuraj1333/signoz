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

.PHONY: all build clean run start debug-alert send-alert

all: clean build

build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) .
	$(GOBUILD) -o $(BUILD_DIR)/$(CLIENT_BINARY_NAME) ./client

clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

run:
	$(GORUN) .

# Build and run the binary
start: build
	./$(BUILD_DIR)/$(BINARY_NAME)

# Debug command
debug-alert:
	@if [ "$(file)" = "" ]; then \
		echo "Error: Alert JSON file is required. Usage: make debug-alert file='path/to/alert.json'"; \
		exit 1; \
	fi
	@if [ ! -f "$(file)" ]; then \
		echo "Error: File '$(file)' does not exist"; \
		exit 1; \
	fi
	$(GORUN) . --debug --alert-file "$(file)"

# Send alert command
send-alert:
	@if [ "$(file)" = "" ]; then \
		echo "Error: Alert JSON file is required. Usage: make send-alert file='path/to/alert.json' [url='http://localhost:8080/webhook']"; \
		exit 1; \
	fi
	@if [ ! -f "$(file)" ]; then \
		echo "Error: File '$(file)' does not exist"; \
		exit 1; \
	fi
	cd client && $(GORUN) . --alert-file "../$(file)" --webhook-url "$(if $(url),$(url),$(DEFAULT_WEBHOOK_URL))" 