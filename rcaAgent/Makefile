# Binary name
BINARY_NAME=rca-agent

# Go parameters
GOCMD=go
GOBUILD=$(GOCMD) build
GORUN=$(GOCMD) run
GOCLEAN=$(GOCMD) clean

# Build directory
BUILD_DIR=build

.PHONY: all build clean run

all: clean build

build:
	mkdir -p $(BUILD_DIR)
	$(GOBUILD) -o $(BUILD_DIR)/$(BINARY_NAME) .

clean:
	$(GOCLEAN)
	rm -rf $(BUILD_DIR)

run:
	$(GORUN) main.go

# Build and run the binary
start: build
	./$(BUILD_DIR)/$(BINARY_NAME) 