FROM golang:1.22-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git make

# Set working directory
WORKDIR /build

# Copy go mod files
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build binary
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -o mindb-server ./cmd/mindb-server

# Final stage
FROM alpine:latest

# Install runtime dependencies
RUN apk --no-cache add ca-certificates tzdata

# Create non-root user
RUN addgroup -g 1000 mindb && \
    adduser -D -u 1000 -G mindb mindb

# Create data directory
RUN mkdir -p /data && chown mindb:mindb /data

# Copy binary from builder
COPY --from=builder /build/mindb-server /usr/local/bin/mindb-server
RUN chmod +x /usr/local/bin/mindb-server

# Switch to non-root user
USER mindb

# Set working directory
WORKDIR /data

# Expose HTTP port
EXPOSE 8080

# Environment defaults
ENV MINDB_DATA_DIR=/data
ENV HTTP_ADDR=:8080
ENV LOG_LEVEL=info

# Health check
HEALTHCHECK --interval=30s --timeout=3s --start-period=5s --retries=3 \
  CMD wget --no-verbose --tries=1 --spider http://localhost:8080/health || exit 1

# Run server
CMD ["/usr/local/bin/mindb-server"]
