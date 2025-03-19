#!/bin/bash

# Change to the project root directory
cd "$(dirname "$0")/.."

# Build the rcaAgent
make build

# Run the rcaAgent with the fetch-rules flag
./build/rcaAgent --fetch-rules 