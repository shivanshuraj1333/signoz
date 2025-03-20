FROM golang:1.20-alpine AS builder

WORKDIR /app

# Install dependencies
RUN apk add --no-cache git make

# Copy go.mod and go.sum
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the application
RUN make build

# Create final lightweight image
FROM alpine:3.18

WORKDIR /app

# Install runtime dependencies
RUN apk add --no-cache ca-certificates curl postgresql-client

# Copy binary from builder stage
COPY --from=builder /app/build/rca-agent /app/rca-agent

# Copy any necessary files
COPY .env.example /app/.env
COPY scripts /app/scripts
COPY testdata /app/testdata

# Expose the server port
EXPOSE 8080

# Command to run the application
CMD ["/app/rca-agent"] 