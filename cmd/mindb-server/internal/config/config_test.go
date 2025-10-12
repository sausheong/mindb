package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadFromEnv_MissingDataDir(t *testing.T) {
	// Clear environment
	os.Clearenv()
	
	_, err := LoadFromEnv()
	if err == nil {
		t.Error("Expected error when MINDB_DATA_DIR is missing")
	}
}

func TestLoadFromEnv_Success(t *testing.T) {
	// Set required env var
	os.Setenv("MINDB_DATA_DIR", "/tmp/mindb-test")
	defer os.Clearenv()
	
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv failed: %v", err)
	}
	
	if cfg == nil {
		t.Fatal("Config is nil")
	}
	
	if cfg.DataDir != "/tmp/mindb-test" {
		t.Errorf("Expected DataDir '/tmp/mindb-test', got '%s'", cfg.DataDir)
	}
	
	// Check defaults
	if cfg.HTTPAddr != ":8080" {
		t.Errorf("Expected default HTTPAddr ':8080', got '%s'", cfg.HTTPAddr)
	}
	if cfg.ReadTimeout != 30*time.Second {
		t.Errorf("Expected default ReadTimeout 30s, got %v", cfg.ReadTimeout)
	}
	if cfg.ExecConcurrency != 32 {
		t.Errorf("Expected default ExecConcurrency 32, got %d", cfg.ExecConcurrency)
	}
}

func TestLoadFromEnv_CustomValues(t *testing.T) {
	os.Clearenv()
	os.Setenv("MINDB_DATA_DIR", "/custom/path")
	os.Setenv("HTTP_ADDR", ":9090")
	os.Setenv("READ_TIMEOUT", "60s")
	os.Setenv("EXEC_CONCURRENCY", "64")
	os.Setenv("ENABLE_TLS", "true")
	os.Setenv("TLS_CERT_FILE", "/path/to/cert.pem")
	os.Setenv("TLS_KEY_FILE", "/path/to/key.pem")
	os.Setenv("AUTH_DISABLED", "true")
	os.Setenv("API_KEY", "test-api-key")
	os.Setenv("LOG_LEVEL", "debug")
	defer os.Clearenv()
	
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv failed: %v", err)
	}
	
	if cfg.DataDir != "/custom/path" {
		t.Errorf("Expected DataDir '/custom/path', got '%s'", cfg.DataDir)
	}
	if cfg.HTTPAddr != ":9090" {
		t.Errorf("Expected HTTPAddr ':9090', got '%s'", cfg.HTTPAddr)
	}
	if cfg.ReadTimeout != 60*time.Second {
		t.Errorf("Expected ReadTimeout 60s, got %v", cfg.ReadTimeout)
	}
	if cfg.ExecConcurrency != 64 {
		t.Errorf("Expected ExecConcurrency 64, got %d", cfg.ExecConcurrency)
	}
	if !cfg.EnableTLS {
		t.Error("Expected EnableTLS true")
	}
	if cfg.TLSCertFile != "/path/to/cert.pem" {
		t.Errorf("Expected TLSCertFile '/path/to/cert.pem', got '%s'", cfg.TLSCertFile)
	}
	if cfg.TLSKeyFile != "/path/to/key.pem" {
		t.Errorf("Expected TLSKeyFile '/path/to/key.pem', got '%s'", cfg.TLSKeyFile)
	}
	if !cfg.AuthDisabled {
		t.Error("Expected AuthDisabled true")
	}
	if cfg.APIKey != "test-api-key" {
		t.Errorf("Expected APIKey 'test-api-key', got '%s'", cfg.APIKey)
	}
	if cfg.LogLevel != "debug" {
		t.Errorf("Expected LogLevel 'debug', got '%s'", cfg.LogLevel)
	}
}

func TestGetEnv(t *testing.T) {
	os.Clearenv()
	
	// Test with existing env var
	os.Setenv("TEST_VAR", "test-value")
	value := getEnv("TEST_VAR", "default")
	if value != "test-value" {
		t.Errorf("Expected 'test-value', got '%s'", value)
	}
	
	// Test with missing env var (should return default)
	value = getEnv("MISSING_VAR", "default-value")
	if value != "default-value" {
		t.Errorf("Expected 'default-value', got '%s'", value)
	}
	
	os.Clearenv()
}

func TestGetInt(t *testing.T) {
	os.Clearenv()
	
	// Test with valid int
	os.Setenv("TEST_INT", "42")
	value := getInt("TEST_INT", 10)
	if value != 42 {
		t.Errorf("Expected 42, got %d", value)
	}
	
	// Test with invalid int (should return default)
	os.Setenv("TEST_INT", "invalid")
	value = getInt("TEST_INT", 10)
	if value != 10 {
		t.Errorf("Expected default 10, got %d", value)
	}
	
	// Test with missing env var (should return default)
	value = getInt("MISSING_INT", 20)
	if value != 20 {
		t.Errorf("Expected default 20, got %d", value)
	}
	
	os.Clearenv()
}

func TestGetBool(t *testing.T) {
	os.Clearenv()
	
	// Test with "true"
	os.Setenv("TEST_BOOL", "true")
	value := getBool("TEST_BOOL", false)
	if !value {
		t.Error("Expected true")
	}
	
	// Test with "1"
	os.Setenv("TEST_BOOL", "1")
	value = getBool("TEST_BOOL", false)
	if !value {
		t.Error("Expected true for '1'")
	}
	
	// Test with "false"
	os.Setenv("TEST_BOOL", "false")
	value = getBool("TEST_BOOL", true)
	if value {
		t.Error("Expected false")
	}
	
	// Test with invalid value (should return default)
	os.Setenv("TEST_BOOL", "invalid")
	value = getBool("TEST_BOOL", true)
	if !value {
		t.Error("Expected default true")
	}
	
	// Test with missing env var (should return default)
	value = getBool("MISSING_BOOL", false)
	if value {
		t.Error("Expected default false")
	}
	
	os.Clearenv()
}

func TestGetDuration(t *testing.T) {
	os.Clearenv()
	
	// Test with valid duration
	os.Setenv("TEST_DURATION", "5s")
	value := getDuration("TEST_DURATION", 10*time.Second)
	if value != 5*time.Second {
		t.Errorf("Expected 5s, got %v", value)
	}
	
	// Test with milliseconds
	os.Setenv("TEST_DURATION", "500ms")
	value = getDuration("TEST_DURATION", 10*time.Second)
	if value != 500*time.Millisecond {
		t.Errorf("Expected 500ms, got %v", value)
	}
	
	// Test with invalid duration (should return default)
	os.Setenv("TEST_DURATION", "invalid")
	value = getDuration("TEST_DURATION", 10*time.Second)
	if value != 10*time.Second {
		t.Errorf("Expected default 10s, got %v", value)
	}
	
	// Test with missing env var (should return default)
	value = getDuration("MISSING_DURATION", 20*time.Second)
	if value != 20*time.Second {
		t.Errorf("Expected default 20s, got %v", value)
	}
	
	os.Clearenv()
}

func TestLoadFromEnv_AllTimeouts(t *testing.T) {
	os.Clearenv()
	os.Setenv("MINDB_DATA_DIR", "/tmp/test")
	os.Setenv("WRITE_TIMEOUT", "45s")
	os.Setenv("IDLE_TIMEOUT", "180s")
	os.Setenv("SHUTDOWN_GRACE", "60s")
	os.Setenv("STMT_TIMEOUT_MS", "5000ms")
	os.Setenv("TX_IDLE_TIMEOUT_MS", "120000ms")
	defer os.Clearenv()
	
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv failed: %v", err)
	}
	
	if cfg.WriteTimeout != 45*time.Second {
		t.Errorf("Expected WriteTimeout 45s, got %v", cfg.WriteTimeout)
	}
	if cfg.IdleTimeout != 180*time.Second {
		t.Errorf("Expected IdleTimeout 180s, got %v", cfg.IdleTimeout)
	}
	if cfg.ShutdownGrace != 60*time.Second {
		t.Errorf("Expected ShutdownGrace 60s, got %v", cfg.ShutdownGrace)
	}
	if cfg.StmtTimeout != 5000*time.Millisecond {
		t.Errorf("Expected StmtTimeout 5000ms, got %v", cfg.StmtTimeout)
	}
	if cfg.TxIdleTimeout != 120000*time.Millisecond {
		t.Errorf("Expected TxIdleTimeout 120000ms, got %v", cfg.TxIdleTimeout)
	}
}

func TestLoadFromEnv_TransactionLimits(t *testing.T) {
	os.Clearenv()
	os.Setenv("MINDB_DATA_DIR", "/tmp/test")
	os.Setenv("MAX_OPEN_TX", "200")
	os.Setenv("MAX_TX_PER_CLIENT", "10")
	defer os.Clearenv()
	
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv failed: %v", err)
	}
	
	if cfg.MaxOpenTx != 200 {
		t.Errorf("Expected MaxOpenTx 200, got %d", cfg.MaxOpenTx)
	}
	if cfg.MaxTxPerClient != 10 {
		t.Errorf("Expected MaxTxPerClient 10, got %d", cfg.MaxTxPerClient)
	}
}

func TestLoadFromEnv_Metrics(t *testing.T) {
	os.Clearenv()
	os.Setenv("MINDB_DATA_DIR", "/tmp/test")
	os.Setenv("ENABLE_METRICS", "false")
	defer os.Clearenv()
	
	cfg, err := LoadFromEnv()
	if err != nil {
		t.Fatalf("LoadFromEnv failed: %v", err)
	}
	
	if cfg.EnableMetrics {
		t.Error("Expected EnableMetrics false")
	}
}
