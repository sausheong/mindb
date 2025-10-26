package api

import (
	"encoding/base64"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/sausheong/mindb/src/server/internal/db"
)

func newTestAdapter(t *testing.T) *db.Adapter {
	tmpDir, err := os.MkdirTemp("", "mindb-auth-test-*")
	if err != nil {
		t.Fatalf("Failed to create temp dir: %v", err)
	}
	t.Cleanup(func() { os.RemoveAll(tmpDir) })

	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	t.Cleanup(func() { adapter.Close() })

	return adapter
}

func basicAuth(username, password string) string {
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth))
}

func TestAuthMiddleware_Success(t *testing.T) {
	adapter := newTestAdapter(t)
	middleware := AuthMiddleware(adapter)
	
	// Create a test handler
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Check that user is in context
		user := r.Context().Value("user")
		if user == nil {
			t.Error("Expected user in context")
		}
		w.WriteHeader(http.StatusOK)
	})
	
	handler := middleware(nextHandler)
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", basicAuth("root", "root"))
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestAuthMiddleware_NoCredentials(t *testing.T) {
	adapter := newTestAdapter(t)
	middleware := AuthMiddleware(adapter)
	
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not reach next handler")
	})
	
	handler := middleware(nextHandler)
	
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
	}
	
	if w.Header().Get("WWW-Authenticate") == "" {
		t.Error("Expected WWW-Authenticate header")
	}
}

func TestAuthMiddleware_InvalidCredentials(t *testing.T) {
	adapter := newTestAdapter(t)
	middleware := AuthMiddleware(adapter)
	
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Should not reach next handler")
	})
	
	handler := middleware(nextHandler)
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", basicAuth("invalid", "wrong"))
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Expected status 401, got %d", w.Code)
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
	
	// After removing port, should be ::1 which maps to %
	if ip != "%" {
		t.Errorf("Expected wildcard '%%', got '%s'", ip)
	}
}

func TestOptionalAuthMiddleware_WithValidAuth(t *testing.T) {
	adapter := newTestAdapter(t)
	middleware := OptionalAuthMiddleware(adapter)
	
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := r.Context().Value("user")
		if user != "root@%" {
			t.Errorf("Expected user 'root@%%', got '%v'", user)
		}
		w.WriteHeader(http.StatusOK)
	})
	
	handler := middleware(nextHandler)
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", basicAuth("root", "root"))
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestOptionalAuthMiddleware_WithInvalidAuth(t *testing.T) {
	adapter := newTestAdapter(t)
	middleware := OptionalAuthMiddleware(adapter)
	
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := r.Context().Value("user")
		if user != "root@%" {
			t.Errorf("Expected fallback user 'root@%%', got '%v'", user)
		}
		w.WriteHeader(http.StatusOK)
	})
	
	handler := middleware(nextHandler)
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.Header.Set("Authorization", basicAuth("invalid", "wrong"))
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}

func TestOptionalAuthMiddleware_NoAuth(t *testing.T) {
	adapter := newTestAdapter(t)
	middleware := OptionalAuthMiddleware(adapter)
	
	nextHandler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		user := r.Context().Value("user")
		if user != "root@%" {
			t.Errorf("Expected default user 'root@%%', got '%v'", user)
		}
		w.WriteHeader(http.StatusOK)
	})
	
	handler := middleware(nextHandler)
	
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}
}
