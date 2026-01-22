# Build stage
FROM golang:1.25-alpine AS builder

WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build the gateway binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o gateway .

# Runtime stage
FROM alpine:latest

# Install ca-certificates for HTTPS
RUN apk --no-cache add ca-certificates

WORKDIR /app

# Copy binary and config files
COPY --from=builder /app/gateway .
COPY --from=builder /app/config.yaml .
COPY --from=builder /app/otel-config.yaml .

# Expose gateway port and metrics port
EXPOSE 8081 8889

# Run the gateway
CMD ["./gateway"]
