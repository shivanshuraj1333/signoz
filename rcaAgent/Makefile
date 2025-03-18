# Binary name
BINARY_NAME=rca-agent

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean

# Build directory
BUILD_DIR=build

.PHONY: all build clean run start debug-url debug-alert

all: clean build

build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) .

clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

run:
	$(GORUN) .

# Build and run the binary
start: build
	./$(BUILD_DIR)/$(BINARY_NAME)

# Debug commands
debug-url:
	@if [ "$(url)" = "" ]; then \
		echo "Error: URL is required. Usage: make debug-url url='YOUR_URL'"; \
		exit 1; \
	fi
	$(GORUN) . --debug --url "$(url)"

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