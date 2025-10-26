package middleware

import (
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestSecurityHeadersMiddleware(t *testing.T) {
	middleware := SecurityHeadersMiddleware()
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	
	tests := []struct {
		name      string
		useTLS    bool
		wantHSTS  bool
	}{
		{
			name:     "with HTTPS",
			useTLS:   true,
			wantHSTS: true,
		},
		{
			name:     "without HTTPS",
			useTLS:   false,
			wantHSTS: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", "/test", nil)
			
			// Simulate TLS if needed
			if tt.useTLS {
				req.TLS = &tls.ConnectionState{}
			}
			
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			
			// Check X-Content-Type-Options
			if got := w.Header().Get("X-Content-Type-Options"); got != "nosniff" {
				t.Errorf("X-Content-Type-Options = %q, want %q", got, "nosniff")
			}
			
			// Check X-Frame-Options
			if got := w.Header().Get("X-Frame-Options"); got != "DENY" {
				t.Errorf("X-Frame-Options = %q, want %q", got, "DENY")
			}
			
			// Check X-XSS-Protection
			if got := w.Header().Get("X-XSS-Protection"); got != "1; mode=block" {
				t.Errorf("X-XSS-Protection = %q, want %q", got, "1; mode=block")
			}
			
			// Check HSTS (only with TLS)
			hsts := w.Header().Get("Strict-Transport-Security")
			if tt.wantHSTS {
				expected := "max-age=31536000; includeSubDomains; preload"
				if hsts != expected {
					t.Errorf("Strict-Transport-Security = %q, want %q", hsts, expected)
				}
			} else {
				if hsts != "" {
					t.Errorf("Strict-Transport-Security should not be set without TLS, got %q", hsts)
				}
			}
			
			// Check Content-Security-Policy
			csp := w.Header().Get("Content-Security-Policy")
			if csp == "" {
				t.Error("Content-Security-Policy should be set")
			}
			if !contains(csp, "default-src 'self'") {
				t.Error("CSP should contain default-src 'self'")
			}
			
			// Check Referrer-Policy
			if got := w.Header().Get("Referrer-Policy"); got != "strict-origin-when-cross-origin" {
				t.Errorf("Referrer-Policy = %q, want %q", got, "strict-origin-when-cross-origin")
			}
			
			// Check Permissions-Policy
			pp := w.Header().Get("Permissions-Policy")
			if pp == "" {
				t.Error("Permissions-Policy should be set")
			}
			
			// Verify response body
			if w.Code != http.StatusOK {
				t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
			}
			if w.Body.String() != "OK" {
				t.Errorf("Body = %q, want %q", w.Body.String(), "OK")
			}
		})
	}
}

func TestSecurityHeadersMiddleware_AllHeaders(t *testing.T) {
	middleware := SecurityHeadersMiddleware()
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.TLS = &tls.ConnectionState{} // Enable TLS
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	// List of all expected headers
	expectedHeaders := []string{
		"X-Content-Type-Options",
		"X-Frame-Options",
		"X-XSS-Protection",
		"Strict-Transport-Security",
		"Content-Security-Policy",
		"Referrer-Policy",
		"Permissions-Policy",
	}
	
	for _, header := range expectedHeaders {
		if w.Header().Get(header) == "" {
			t.Errorf("Header %q should be set", header)
		}
	}
}

func TestHTTPSRedirectMiddleware(t *testing.T) {
	tests := []struct {
		name           string
		httpPort       int
		useTLS         bool
		host           string
		requestURI     string
		wantRedirect   bool
		wantLocation   string
	}{
		{
			name:         "redirect HTTP to HTTPS",
			httpPort:     80,
			useTLS:       false,
			host:         "example.com",
			requestURI:   "/test?foo=bar",
			wantRedirect: true,
			wantLocation: "https://example.com/test?foo=bar",
		},
		{
			name:         "already HTTPS - no redirect",
			httpPort:     80,
			useTLS:       true,
			host:         "example.com",
			requestURI:   "/test",
			wantRedirect: false,
		},
		{
			name:         "redirect disabled (port 0)",
			httpPort:     0,
			useTLS:       false,
			host:         "example.com",
			requestURI:   "/test",
			wantRedirect: false,
		},
		{
			name:         "redirect with custom port",
			httpPort:     8080,
			useTLS:       false,
			host:         "example.com:8080",
			requestURI:   "/api/test",
			wantRedirect: true,
			wantLocation: "https://example.com:8080/api/test",
		},
		{
			name:         "redirect root path",
			httpPort:     80,
			useTLS:       false,
			host:         "example.com",
			requestURI:   "/",
			wantRedirect: true,
			wantLocation: "https://example.com/",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			middleware := HTTPSRedirectMiddleware(tt.httpPort)
			
			handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("OK"))
			}))
			
			req := httptest.NewRequest("GET", tt.requestURI, nil)
			req.Host = tt.host
			
			if tt.useTLS {
				req.TLS = &tls.ConnectionState{}
			}
			
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			
			if tt.wantRedirect {
				if w.Code != http.StatusMovedPermanently {
					t.Errorf("Status code = %d, want %d", w.Code, http.StatusMovedPermanently)
				}
				
				location := w.Header().Get("Location")
				if location != tt.wantLocation {
					t.Errorf("Location = %q, want %q", location, tt.wantLocation)
				}
			} else {
				if w.Code != http.StatusOK {
					t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
				}
				
				if w.Body.String() != "OK" {
					t.Errorf("Body = %q, want %q", w.Body.String(), "OK")
				}
			}
		})
	}
}

func TestHTTPSRedirectMiddleware_PreservesQueryParams(t *testing.T) {
	middleware := HTTPSRedirectMiddleware(80)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	
	req := httptest.NewRequest("GET", "/test?param1=value1&param2=value2", nil)
	req.Host = "example.com"
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusMovedPermanently {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusMovedPermanently)
	}
	
	location := w.Header().Get("Location")
	expected := "https://example.com/test?param1=value1&param2=value2"
	if location != expected {
		t.Errorf("Location = %q, want %q", location, expected)
	}
}

func TestHTTPSRedirectMiddleware_PreservesFragment(t *testing.T) {
	middleware := HTTPSRedirectMiddleware(80)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	
	req := httptest.NewRequest("GET", "/test#section", nil)
	req.Host = "example.com"
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	location := w.Header().Get("Location")
	expected := "https://example.com/test#section"
	if location != expected {
		t.Errorf("Location = %q, want %q", location, expected)
	}
}

func TestSecurityHeadersMiddleware_DoesNotOverwriteExisting(t *testing.T) {
	middleware := SecurityHeadersMiddleware()
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Handler sets its own header
		w.Header().Set("X-Custom-Header", "custom-value")
		w.WriteHeader(http.StatusOK)
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.TLS = &tls.ConnectionState{}
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	// Verify custom header is preserved
	if got := w.Header().Get("X-Custom-Header"); got != "custom-value" {
		t.Errorf("X-Custom-Header = %q, want %q", got, "custom-value")
	}
	
	// Verify security headers are still set
	if w.Header().Get("X-Content-Type-Options") == "" {
		t.Error("Security headers should still be set")
	}
}

func TestSecurityHeadersMiddleware_MultipleRequests(t *testing.T) {
	middleware := SecurityHeadersMiddleware()
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	
	// Make multiple requests
	for i := 0; i < 10; i++ {
		req := httptest.NewRequest("GET", "/test", nil)
		req.TLS = &tls.ConnectionState{}
		w := httptest.NewRecorder()
		
		handler.ServeHTTP(w, req)
		
		if w.Header().Get("X-Content-Type-Options") != "nosniff" {
			t.Errorf("Request %d: X-Content-Type-Options not set correctly", i)
		}
	}
}

func TestHTTPSRedirectMiddleware_DifferentMethods(t *testing.T) {
	middleware := HTTPSRedirectMiddleware(80)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	
	methods := []string{"GET", "POST", "PUT", "DELETE", "PATCH", "HEAD", "OPTIONS"}
	
	for _, method := range methods {
		t.Run(method, func(t *testing.T) {
			req := httptest.NewRequest(method, "/test", nil)
			req.Host = "example.com"
			
			w := httptest.NewRecorder()
			handler.ServeHTTP(w, req)
			
			if w.Code != http.StatusMovedPermanently {
				t.Errorf("Method %s: Status code = %d, want %d", method, w.Code, http.StatusMovedPermanently)
			}
		})
	}
}

func TestSecurityHeadersMiddleware_CSPContent(t *testing.T) {
	middleware := SecurityHeadersMiddleware()
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	csp := w.Header().Get("Content-Security-Policy")
	
	// Check for important CSP directives
	requiredDirectives := []string{
		"default-src 'self'",
		"script-src",
		"style-src",
		"img-src",
		"font-src",
		"connect-src",
		"frame-ancestors 'none'",
	}
	
	for _, directive := range requiredDirectives {
		if !contains(csp, directive) {
			t.Errorf("CSP should contain %q, got %q", directive, csp)
		}
	}
}

// Helper function
func contains(s, substr string) bool {
	return len(s) >= len(substr) && (s == substr || len(s) > len(substr) && 
		(s[:len(substr)] == substr || s[len(s)-len(substr):] == substr || 
		containsSubstring(s, substr)))
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
