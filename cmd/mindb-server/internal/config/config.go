package config

import (
	"fmt"
	"os"
	"strconv"
	"time"
)

// Config holds all server configuration
type Config struct {
	// Data directory (required)
	DataDir string
	
	// HTTP server
	HTTPAddr       string
	ReadTimeout    time.Duration
	WriteTimeout   time.Duration
	IdleTimeout    time.Duration
	ShutdownGrace  time.Duration
	
	// Execution limits
	ExecConcurrency  int
	StmtTimeout      time.Duration
	TxIdleTimeout    time.Duration
	MaxOpenTx        int
	MaxTxPerClient   int
	
	// Auth
	AuthDisabled bool
	APIKey       string
	
	// Observability
	EnableMetrics bool
	LogLevel      string
}

// LoadFromEnv loads configuration from environment variables
func LoadFromEnv() (*Config, error) {
	dataDir := os.Getenv("MINDB_DATA_DIR")
	if dataDir == "" {
		return nil, fmt.Errorf("MINDB_DATA_DIR is required")
	}

	cfg := &Config{
		DataDir:         dataDir,
		HTTPAddr:        getEnv("HTTP_ADDR", ":8080"),
		ReadTimeout:     getDuration("READ_TIMEOUT", 30*time.Second),
		WriteTimeout:    getDuration("WRITE_TIMEOUT", 30*time.Second),
		IdleTimeout:     getDuration("IDLE_TIMEOUT", 120*time.Second),
		ShutdownGrace:   getDuration("SHUTDOWN_GRACE", 30*time.Second),
		ExecConcurrency: getInt("EXEC_CONCURRENCY", 32),
		StmtTimeout:     getDuration("STMT_TIMEOUT_MS", 2000*time.Millisecond),
		TxIdleTimeout:   getDuration("TX_IDLE_TIMEOUT_MS", 60000*time.Millisecond),
		MaxOpenTx:       getInt("MAX_OPEN_TX", 100),
		MaxTxPerClient:  getInt("MAX_TX_PER_CLIENT", 5),
		AuthDisabled:    getBool("AUTH_DISABLED", false),
		APIKey:          os.Getenv("API_KEY"),
		EnableMetrics:   getBool("ENABLE_METRICS", true),
		LogLevel:        getEnv("LOG_LEVEL", "info"),
	}

	return cfg, nil
}

func getEnv(key, defaultValue string) string {
	if value := os.Getenv(key); value != "" {
		return value
	}
	return defaultValue
}

func getInt(key string, defaultValue int) int {
	if value := os.Getenv(key); value != "" {
		if i, err := strconv.Atoi(value); err == nil {
			return i
		}
	}
	return defaultValue
}

func getBool(key string, defaultValue bool) bool {
	if value := os.Getenv(key); value != "" {
		if b, err := strconv.ParseBool(value); err == nil {
			return b
		}
	}
	return defaultValue
}

func getDuration(key string, defaultValue time.Duration) time.Duration {
	if value := os.Getenv(key); value != "" {
		// Try parsing as milliseconds first
		if ms, err := strconv.ParseInt(value, 10, 64); err == nil {
			return time.Duration(ms) * time.Millisecond
		}
		// Try parsing as duration string
		if d, err := time.ParseDuration(value); err == nil {
			return d
		}
	}
	return defaultValue
}
