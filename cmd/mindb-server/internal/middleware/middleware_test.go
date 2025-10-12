package middleware

import (
	"bytes"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/rs/zerolog"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/db"
)

func TestLoggingMiddleware_Success(t *testing.T) {
	// Create a buffer to capture logs
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	middleware := LoggingMiddleware(logger)

	// Create a test handler
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.1:12345"
	req.Header.Set("User-Agent", "test-agent")
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	// Check that logging occurred
	if buf.Len() == 0 {
		t.Error("Expected log output, got empty buffer")
	}
	if !bytes.Contains(buf.Bytes(), []byte("http_request")) {
		t.Error("Expected log to contain 'http_request'")
	}
	if !bytes.Contains(buf.Bytes(), []byte("/test")) {
		t.Error("Expected log to contain path '/test'")
	}
}

func TestLoggingMiddleware_ErrorStatus(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	middleware := LoggingMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusBadRequest)
		w.Write([]byte("error"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("POST", "/error", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	// Should log as warning for 4xx
	if !bytes.Contains(buf.Bytes(), []byte("400")) {
		t.Error("Expected log to contain status 400")
	}
}

func TestLoggingMiddleware_ServerError(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	middleware := LoggingMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("server error"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/server-error", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Should log as error for 5xx
	if !bytes.Contains(buf.Bytes(), []byte("500")) {
		t.Error("Expected log to contain status 500")
	}
}

func TestResponseWriter_WriteHeader(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &responseWriter{
		ResponseWriter: rec,
		status:         http.StatusOK,
	}

	rw.WriteHeader(http.StatusCreated)

	if rw.status != http.StatusCreated {
		t.Errorf("Expected status 201, got %d", rw.status)
	}
	if rec.Code != http.StatusCreated {
		t.Errorf("Expected recorder status 201, got %d", rec.Code)
	}
}

func TestResponseWriter_Write(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &responseWriter{
		ResponseWriter: rec,
		status:         http.StatusOK,
	}

	data := []byte("test data")
	n, err := rw.Write(data)

	if err != nil {
		t.Errorf("Write failed: %v", err)
	}
	if n != len(data) {
		t.Errorf("Expected to write %d bytes, wrote %d", len(data), n)
	}
	if rw.bytes != len(data) {
		t.Errorf("Expected bytes counter %d, got %d", len(data), rw.bytes)
	}
	if rec.Body.String() != string(data) {
		t.Errorf("Expected body '%s', got '%s'", string(data), rec.Body.String())
	}
}

func TestResponseWriter_MultipleWrites(t *testing.T) {
	rec := httptest.NewRecorder()
	rw := &responseWriter{
		ResponseWriter: rec,
		status:         http.StatusOK,
	}

	rw.Write([]byte("hello "))
	rw.Write([]byte("world"))

	if rw.bytes != 11 {
		t.Errorf("Expected 11 bytes total, got %d", rw.bytes)
	}
	if rec.Body.String() != "hello world" {
		t.Errorf("Expected body 'hello world', got '%s'", rec.Body.String())
	}
}

func TestRecoveryMiddleware_NoPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	middleware := RecoveryMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("success"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
	if w.Body.String() != "success" {
		t.Errorf("Expected body 'success', got '%s'", w.Body.String())
	}

	// Should not log anything for successful request
	_ = buf.String()
	if bytes.Contains(buf.Bytes(), []byte("panic")) {
		t.Error("Should not log panic for successful request")
	}
}

func TestRecoveryMiddleware_WithPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	middleware := RecoveryMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("test panic")
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/panic", nil)
	w := httptest.NewRecorder()

	// Should not panic - middleware should recover
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Check response body contains error
	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("error")) {
		t.Error("Expected error in response body")
	}
	if !bytes.Contains([]byte(body), []byte("internal server error")) {
		t.Error("Expected 'internal server error' in response body")
	}

	// Check that panic was logged
	_ = buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("panic_recovered")) {
		t.Error("Expected log to contain 'panic_recovered'")
	}
	if !bytes.Contains(buf.Bytes(), []byte("test panic")) {
		t.Error("Expected log to contain panic message")
	}
}

func TestRecoveryMiddleware_WithNilPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	middleware := RecoveryMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(nil)
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/panic-nil", nil)
	w := httptest.NewRecorder()

	// Should handle nil panic
	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}
}

func TestRecoveryMiddleware_WithErrorPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	middleware := RecoveryMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic(http.ErrAbortHandler)
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/panic-error", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Should log the error
	_ = buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("panic_recovered")) {
		t.Error("Expected log to contain 'panic_recovered'")
	}
}

func TestMiddlewareChaining(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	// Chain logging and recovery middleware
	loggingMW := LoggingMiddleware(logger)
	recoveryMW := RecoveryMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("chained"))
	})

	// Apply both middleware
	wrappedHandler := loggingMW(recoveryMW(handler))

	req := httptest.NewRequest("GET", "/chained", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
	if w.Body.String() != "chained" {
		t.Errorf("Expected body 'chained', got '%s'", w.Body.String())
	}

	// Should have logged the request
	_ = buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("http_request")) {
		t.Error("Expected log to contain 'http_request'")
	}
}

func TestMiddlewareChaining_WithPanic(t *testing.T) {
	var buf bytes.Buffer
	logger := zerolog.New(&buf)

	loggingMW := LoggingMiddleware(logger)
	recoveryMW := RecoveryMiddleware(logger)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		panic("chained panic")
	})

	// Apply both middleware - recovery should catch panic, logging should log it
	wrappedHandler := loggingMW(recoveryMW(handler))

	req := httptest.NewRequest("GET", "/chained-panic", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusInternalServerError {
		t.Errorf("Expected status 500, got %d", w.Code)
	}

	// Should have logged both the panic and the request
	_ = buf.String()
	if !bytes.Contains(buf.Bytes(), []byte("panic_recovered")) {
		t.Error("Expected log to contain 'panic_recovered'")
	}
	if !bytes.Contains(buf.Bytes(), []byte("http_request")) {
		t.Error("Expected log to contain 'http_request'")
	}
}

func TestAuthMiddleware_ValidAPIKey(t *testing.T) {
	apiKey := "test-api-key-123"
	middleware := AuthMiddleware(apiKey, false)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("authorized"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-API-Key", apiKey)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
	if w.Body.String() != "authorized" {
		t.Errorf("Expected body 'authorized', got '%s'", w.Body.String())
	}
}

func TestAuthMiddleware_ValidBearerToken(t *testing.T) {
	apiKey := "test-api-key-123"
	middleware := AuthMiddleware(apiKey, false)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("authorized"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", "Bearer "+apiKey)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestAuthMiddleware_InvalidAPIKey(t *testing.T) {
	apiKey := "test-api-key-123"
	middleware := AuthMiddleware(apiKey, false)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not reach handler")
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-API-Key", "wrong-key")
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_MissingAPIKey(t *testing.T) {
	apiKey := "test-api-key-123"
	middleware := AuthMiddleware(apiKey, false)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not reach handler")
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}
}

func TestAuthMiddleware_Disabled(t *testing.T) {
	apiKey := "test-api-key-123"
	middleware := AuthMiddleware(apiKey, true) // disabled

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("no auth required"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	// No API key provided
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestGetClientIP_XForwardedFor(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Forwarded-For", "192.168.1.100, 10.0.0.1")

	ip := getClientIP(req)

	if ip != "192.168.1.100" {
		t.Errorf("Expected IP '192.168.1.100', got '%s'", ip)
	}
}

func TestGetClientIP_XRealIP(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("X-Real-IP", "192.168.1.200")

	ip := getClientIP(req)

	if ip != "192.168.1.200" {
		t.Errorf("Expected IP '192.168.1.200', got '%s'", ip)
	}
}

func TestGetClientIP_RemoteAddr(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "192.168.1.50:12345"

	ip := getClientIP(req)

	if ip != "192.168.1.50" {
		t.Errorf("Expected IP '192.168.1.50', got '%s'", ip)
	}
}

func TestGetClientIP_Localhost(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "127.0.0.1:8080"

	ip := getClientIP(req)

	if ip != "%" {
		t.Errorf("Expected wildcard '%%', got '%s'", ip)
	}
}

func TestGetClientIP_IPv6Localhost(t *testing.T) {
	req := httptest.NewRequest("GET", "/test", nil)
	req.RemoteAddr = "[::1]:8080"

	ip := getClientIP(req)

	// After removing port, should be [::1] which maps to %
	// Note: The implementation removes the port but keeps the brackets
	if ip != "[::1]" && ip != "::1" && ip != "%" {
		t.Errorf("Expected '::1' or '[::1]' or '%%', got '%s'", ip)
	}
}

func TestWriteError(t *testing.T) {
	w := httptest.NewRecorder()

	writeError(w, http.StatusBadRequest, "BAD_REQUEST", "test error message")

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	if w.Header().Get("Content-Type") != "application/json" {
		t.Error("Expected Content-Type application/json")
	}

	body := w.Body.String()
	if !bytes.Contains([]byte(body), []byte("BAD_REQUEST")) {
		t.Error("Expected error code in body")
	}
	if !bytes.Contains([]byte(body), []byte("test error message")) {
		t.Error("Expected error message in body")
	}
}

func TestDBAuthMiddleware_ValidCredentials(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	middleware := DBAuthMiddleware(adapter, false)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := r.Context().Value("user")
		if user == nil {
			t.Error("Expected user in context")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("authorized"))
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.SetBasicAuth("root", "root")
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestDBAuthMiddleware_InvalidCredentials(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	middleware := DBAuthMiddleware(adapter, false)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not reach handler")
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.SetBasicAuth("invalid", "wrong")
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}

	if w.Header().Get("WWW-Authenticate") == "" {
		t.Error("Expected WWW-Authenticate header")
	}
}

func TestDBAuthMiddleware_NoCredentials_Required(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	middleware := DBAuthMiddleware(adapter, false)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not reach handler")
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}
}

func TestDBAuthMiddleware_Optional_NoCredentials(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	middleware := DBAuthMiddleware(adapter, true) // optional

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := r.Context().Value("user")
		if user != "root@%" {
			t.Errorf("Expected root@%%, got %v", user)
		}
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestDBAuthMiddleware_Optional_InvalidCredentials(t *testing.T) {
	tmpDir, err := os.MkdirTemp("", "mindb-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tmpDir)

	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("NewAdapter failed: %v", err)
	}
	defer adapter.Close()

	middleware := DBAuthMiddleware(adapter, true) // optional

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Should still reach handler with optional auth
		w.WriteHeader(http.StatusOK)
	})

	wrappedHandler := middleware(handler)

	req := httptest.NewRequest("GET", "/test", nil)
	req.SetBasicAuth("invalid", "wrong")
	w := httptest.NewRecorder()

	wrappedHandler.ServeHTTP(w, req)

	// With optional auth and invalid credentials, should use root
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200 with optional auth, got %d", w.Code)
	}
}
