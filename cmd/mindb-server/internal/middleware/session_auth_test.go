package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/sausheong/mindb/cmd/mindb-server/internal/db"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/session"
)

func TestSessionAuthMiddleware_WithValidSession(t *testing.T) {
	// Create test database
	adapter, cleanup := createTestDB(t)
	defer cleanup()
	
	// Create session manager
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	
	// Create session
	token, err := sessionMgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	// Create middleware
	middleware := SessionAuthMiddleware(adapter, sessionMgr, false)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify context has user info
		user := r.Context().Value("user")
		if user == nil {
			t.Error("User should be in context")
		}
		
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  SessionCookieName,
		Value: token,
	})
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestSessionAuthMiddleware_WithExpiredSession(t *testing.T) {
	adapter, cleanup := createTestDB(t)
	defer cleanup()
	
	// Create session manager with short timeout
	sessionMgr := session.NewManager("test-key", 1*time.Millisecond)
	
	// Create session
	token, err := sessionMgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	// Wait for session to expire
	time.Sleep(10 * time.Millisecond)
	
	middleware := SessionAuthMiddleware(adapter, sessionMgr, false)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called with expired session")
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  SessionCookieName,
		Value: token,
	})
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	// Should return unauthorized
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestSessionAuthMiddleware_WithBasicAuth(t *testing.T) {
	adapter, cleanup := createTestDB(t)
	defer cleanup()
	
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	// Use optional=true to allow root user without password
	middleware := SessionAuthMiddleware(adapter, sessionMgr, true)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Verify user is set in context
		user := r.Context().Value("user")
		if user == nil {
			t.Error("User should be in context")
		}
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("OK"))
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.SetBasicAuth("root", "")
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d. Body: %s", w.Code, http.StatusOK, w.Body.String())
	}
	
	// Note: Session cookie is only set when authentication succeeds
	// With root user and empty password, it depends on whether authentication passes
	// The middleware should at least set the user in context (verified above)
}

func TestSessionAuthMiddleware_NoAuth_Required(t *testing.T) {
	adapter, cleanup := createTestDB(t)
	defer cleanup()
	
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	middleware := SessionAuthMiddleware(adapter, sessionMgr, false) // required
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called without auth")
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestSessionAuthMiddleware_NoAuth_Optional(t *testing.T) {
	adapter, cleanup := createTestDB(t)
	defer cleanup()
	
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	middleware := SessionAuthMiddleware(adapter, sessionMgr, true) // optional
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestLogoutHandler(t *testing.T) {
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	
	// Create session
	token, err := sessionMgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	handler := LogoutHandler(sessionMgr)
	
	req := httptest.NewRequest("POST", "/auth/logout", nil)
	req.AddCookie(&http.Cookie{
		Name:  SessionCookieName,
		Value: token,
	})
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
	
	// Verify session was revoked
	_, err = sessionMgr.ValidateSession(token)
	if err == nil {
		t.Error("Session should be invalid after logout")
	}
	
	// Verify cookie was cleared
	cookies := w.Result().Cookies()
	for _, cookie := range cookies {
		if cookie.Name == SessionCookieName {
			if cookie.MaxAge != -1 {
				t.Error("Session cookie should be deleted (MaxAge=-1)")
			}
		}
	}
}

func TestLogoutHandler_NoCookie(t *testing.T) {
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	handler := LogoutHandler(sessionMgr)
	
	req := httptest.NewRequest("POST", "/auth/logout", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	// Should still return OK even without cookie
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
}

func TestRefreshSessionHandler(t *testing.T) {
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	
	// Create session
	token, err := sessionMgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	handler := RefreshSessionHandler(sessionMgr)
	
	req := httptest.NewRequest("POST", "/auth/refresh", nil)
	req.AddCookie(&http.Cookie{
		Name:  SessionCookieName,
		Value: token,
	})
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusOK {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusOK)
	}
	
	// Verify new cookie was set
	cookies := w.Result().Cookies()
	found := false
	for _, cookie := range cookies {
		if cookie.Name == SessionCookieName {
			found = true
			if cookie.Value == token {
				t.Error("New token should be different from old token")
			}
		}
	}
	
	if !found {
		t.Error("New session cookie should be set")
	}
}

func TestRefreshSessionHandler_NoCookie(t *testing.T) {
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	handler := RefreshSessionHandler(sessionMgr)
	
	req := httptest.NewRequest("POST", "/auth/refresh", nil)
	w := httptest.NewRecorder()
	
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestRefreshSessionHandler_ExpiredSession(t *testing.T) {
	sessionMgr := session.NewManager("test-key", 1*time.Millisecond)
	
	// Create session
	token, err := sessionMgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	// Wait for expiration
	time.Sleep(10 * time.Millisecond)
	
	handler := RefreshSessionHandler(sessionMgr)
	
	req := httptest.NewRequest("POST", "/auth/refresh", nil)
	req.AddCookie(&http.Cookie{
		Name:  SessionCookieName,
		Value: token,
	})
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestSetSessionCookie(t *testing.T) {
	w := httptest.NewRecorder()
	token := "test-token-value"
	
	setSessionCookie(w, token, true)
	
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("Expected 1 cookie, got %d", len(cookies))
	}
	
	cookie := cookies[0]
	
	if cookie.Name != SessionCookieName {
		t.Errorf("Cookie name = %q, want %q", cookie.Name, SessionCookieName)
	}
	
	if cookie.Value != token {
		t.Errorf("Cookie value = %q, want %q", cookie.Value, token)
	}
	
	if !cookie.HttpOnly {
		t.Error("Cookie should be HttpOnly")
	}
	
	if !cookie.Secure {
		t.Error("Cookie should be Secure")
	}
	
	if cookie.SameSite != http.SameSiteStrictMode {
		t.Error("Cookie should have SameSite=Strict")
	}
	
	if cookie.Path != "/" {
		t.Errorf("Cookie path = %q, want %q", cookie.Path, "/")
	}
	
	if cookie.MaxAge != 15*60 {
		t.Errorf("Cookie MaxAge = %d, want %d", cookie.MaxAge, 15*60)
	}
}

func TestSetSessionCookie_NoSecure(t *testing.T) {
	w := httptest.NewRecorder()
	token := "test-token-value"
	
	setSessionCookie(w, token, false) // secure = false
	
	cookies := w.Result().Cookies()
	cookie := cookies[0]
	
	if cookie.Secure {
		t.Error("Cookie should not be Secure when secure=false")
	}
}

func TestClearSessionCookie(t *testing.T) {
	w := httptest.NewRecorder()
	
	clearSessionCookie(w)
	
	cookies := w.Result().Cookies()
	if len(cookies) != 1 {
		t.Fatalf("Expected 1 cookie, got %d", len(cookies))
	}
	
	cookie := cookies[0]
	
	if cookie.Name != SessionCookieName {
		t.Errorf("Cookie name = %q, want %q", cookie.Name, SessionCookieName)
	}
	
	if cookie.Value != "" {
		t.Errorf("Cookie value should be empty, got %q", cookie.Value)
	}
	
	if cookie.MaxAge != -1 {
		t.Errorf("Cookie MaxAge = %d, want -1", cookie.MaxAge)
	}
}

func TestSessionAuthMiddleware_InvalidBasicAuth(t *testing.T) {
	adapter, cleanup := createTestDB(t)
	defer cleanup()
	
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	middleware := SessionAuthMiddleware(adapter, sessionMgr, false)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called with invalid auth")
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.SetBasicAuth("invalid", "invalid")
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	if w.Code != http.StatusUnauthorized {
		t.Errorf("Status code = %d, want %d", w.Code, http.StatusUnauthorized)
	}
}

func TestSessionAuthMiddleware_ClearsInvalidCookie(t *testing.T) {
	adapter, cleanup := createTestDB(t)
	defer cleanup()
	
	sessionMgr := session.NewManager("test-key", 15*time.Minute)
	middleware := SessionAuthMiddleware(adapter, sessionMgr, false)
	
	handler := middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("Handler should not be called")
	}))
	
	req := httptest.NewRequest("GET", "/test", nil)
	req.AddCookie(&http.Cookie{
		Name:  SessionCookieName,
		Value: "invalid-token",
	})
	
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	
	// Verify cookie was cleared
	cookies := w.Result().Cookies()
	for _, cookie := range cookies {
		if cookie.Name == SessionCookieName {
			if cookie.MaxAge != -1 {
				t.Error("Invalid session cookie should be cleared")
			}
		}
	}
}

func TestSessionCleanupWorker(t *testing.T) {
	sessionMgr := session.NewManager("test-key", 50*time.Millisecond)
	
	// Create sessions
	for i := 0; i < 5; i++ {
		_, err := sessionMgr.CreateSession("user", "localhost")
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
	}
	
	// Verify sessions exist
	if count := sessionMgr.GetActiveSessions(); count != 5 {
		t.Errorf("Expected 5 sessions, got %d", count)
	}
	
	// Start cleanup worker
	done := make(chan bool)
	go func() {
		SessionCleanupWorker(sessionMgr, 100*time.Millisecond)
		done <- true
	}()
	
	// Wait for cleanup to run
	time.Sleep(200 * time.Millisecond)
	
	// Verify sessions were cleaned up
	if count := sessionMgr.GetActiveSessions(); count != 0 {
		t.Errorf("Expected 0 sessions after cleanup, got %d", count)
	}
	
	// Note: We can't easily stop the worker in this test, but it will exit when the test ends
}

// Helper function to create test database
func createTestDB(t *testing.T) (*db.Adapter, func()) {
	t.Helper()
	
	// Create temporary directory
	tmpDir := t.TempDir()
	
	// Create adapter
	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create adapter: %v", err)
	}
	
	cleanup := func() {
		adapter.Close()
	}
	
	return adapter, cleanup
}
