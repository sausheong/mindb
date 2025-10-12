package session

import (
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	tests := []struct {
		name           string
		signingKey     string
		sessionTimeout time.Duration
	}{
		{
			name:           "with signing key",
			signingKey:     "test-secret-key",
			sessionTimeout: 15 * time.Minute,
		},
		{
			name:           "empty signing key (auto-generated)",
			signingKey:     "",
			sessionTimeout: 10 * time.Minute,
		},
		{
			name:           "short timeout",
			signingKey:     "short-key",
			sessionTimeout: 1 * time.Minute,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			mgr := NewManager(tt.signingKey, tt.sessionTimeout)
			
			if mgr == nil {
				t.Fatal("NewManager returned nil")
			}
			
			if mgr.sessionTimeout != tt.sessionTimeout {
				t.Errorf("Expected timeout %v, got %v", tt.sessionTimeout, mgr.sessionTimeout)
			}
			
			if len(mgr.signingKey) == 0 {
				t.Error("Signing key should not be empty")
			}
			
			if mgr.sessions == nil {
				t.Error("Sessions map should be initialized")
			}
		})
	}
}

func TestCreateSession(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	tests := []struct {
		name     string
		username string
		host     string
		wantErr  bool
	}{
		{
			name:     "valid session",
			username: "testuser",
			host:     "localhost",
			wantErr:  false,
		},
		{
			name:     "empty username",
			username: "",
			host:     "localhost",
			wantErr:  false, // Should still work
		},
		{
			name:     "wildcard host",
			username: "admin",
			host:     "%",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			token, err := mgr.CreateSession(tt.username, tt.host)
			
			if (err != nil) != tt.wantErr {
				t.Errorf("CreateSession() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr {
				if token == "" {
					t.Error("Expected non-empty token")
				}
				
				// Verify session was stored
				mgr.mu.RLock()
				sessionCount := len(mgr.sessions)
				mgr.mu.RUnlock()
				
				if sessionCount == 0 {
					t.Error("Session should be stored in manager")
				}
			}
		})
	}
}

func TestValidateSession(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	// Create a valid session
	username := "testuser"
	host := "localhost"
	token, err := mgr.CreateSession(username, host)
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{
			name:    "valid token",
			token:   token,
			wantErr: false,
		},
		{
			name:    "empty token",
			token:   "",
			wantErr: true,
		},
		{
			name:    "invalid token",
			token:   "invalid.token.here",
			wantErr: true,
		},
		{
			name:    "malformed token",
			token:   "not-a-jwt-token",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			claims, err := mgr.ValidateSession(tt.token)
			
			if (err != nil) != tt.wantErr {
				t.Errorf("ValidateSession() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr {
				if claims == nil {
					t.Error("Expected non-nil claims")
					return
				}
				
				if claims.Username != username {
					t.Errorf("Expected username %s, got %s", username, claims.Username)
				}
				
				if claims.Host != host {
					t.Errorf("Expected host %s, got %s", host, claims.Host)
				}
			}
		})
	}
}

func TestValidateSession_Expired(t *testing.T) {
	// Create manager with very short timeout
	mgr := NewManager("test-key", 1*time.Millisecond)
	
	token, err := mgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	// Wait for session to expire
	time.Sleep(10 * time.Millisecond)
	
	// Try to validate expired session
	_, err = mgr.ValidateSession(token)
	if err == nil {
		t.Error("Expected error for expired session")
	}
}

func TestValidateSession_UpdatesLastAccess(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	token, err := mgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	// Get initial last access time
	claims1, err := mgr.ValidateSession(token)
	if err != nil {
		t.Fatalf("Failed to validate session: %v", err)
	}
	
	mgr.mu.RLock()
	sessionInfo := mgr.sessions[claims1.ID]
	firstAccess := sessionInfo.LastAccess
	mgr.mu.RUnlock()
	
	// Wait a bit
	time.Sleep(10 * time.Millisecond)
	
	// Validate again
	_, err = mgr.ValidateSession(token)
	if err != nil {
		t.Fatalf("Failed to validate session: %v", err)
	}
	
	// Check that last access was updated
	mgr.mu.RLock()
	secondAccess := mgr.sessions[claims1.ID].LastAccess
	mgr.mu.RUnlock()
	
	if !secondAccess.After(firstAccess) {
		t.Error("LastAccess should be updated on validation")
	}
}

func TestRefreshSession(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	// Create initial session
	token, err := mgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	tests := []struct {
		name    string
		token   string
		wantErr bool
	}{
		{
			name:    "valid refresh",
			token:   token,
			wantErr: false,
		},
		{
			name:    "invalid token",
			token:   "invalid.token.here",
			wantErr: true,
		},
		{
			name:    "empty token",
			token:   "",
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			newToken, err := mgr.RefreshSession(tt.token)
			
			if (err != nil) != tt.wantErr {
				t.Errorf("RefreshSession() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			
			if !tt.wantErr {
				if newToken == "" {
					t.Error("Expected non-empty new token")
				}
				
				if newToken == tt.token {
					t.Error("New token should be different from old token")
				}
				
				// Verify new token is valid
				claims, err := mgr.ValidateSession(newToken)
				if err != nil {
					t.Errorf("New token should be valid: %v", err)
				}
				
				if claims.Username != "testuser" {
					t.Error("New token should have same username")
				}
			}
		})
	}
}

func TestRevokeSession(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	// Create session
	token, err := mgr.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	// Verify session exists
	_, err = mgr.ValidateSession(token)
	if err != nil {
		t.Fatalf("Session should be valid: %v", err)
	}
	
	// Revoke session
	err = mgr.RevokeSession(token)
	if err != nil {
		t.Errorf("RevokeSession() error = %v", err)
	}
	
	// Verify session is gone
	_, err = mgr.ValidateSession(token)
	if err == nil {
		t.Error("Session should be invalid after revocation")
	}
}

func TestRevokeSession_InvalidToken(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	tests := []struct {
		name  string
		token string
	}{
		{
			name:  "invalid token",
			token: "invalid.token.here",
		},
		{
			name:  "empty token",
			token: "",
		},
		{
			name:  "malformed token",
			token: "not-a-jwt",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := mgr.RevokeSession(tt.token)
			// Should not panic, may return error
			_ = err
		})
	}
}

func TestCleanupExpiredSessions(t *testing.T) {
	mgr := NewManager("test-key", 50*time.Millisecond)
	
	// Create multiple sessions
	for i := 0; i < 5; i++ {
		_, err := mgr.CreateSession("user"+string(rune(i)), "localhost")
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
	}
	
	// Verify sessions exist
	initialCount := mgr.GetActiveSessions()
	if initialCount != 5 {
		t.Errorf("Expected 5 sessions, got %d", initialCount)
	}
	
	// Wait for sessions to expire
	time.Sleep(100 * time.Millisecond)
	
	// Cleanup expired sessions
	mgr.CleanupExpiredSessions()
	
	// Verify sessions were cleaned up
	finalCount := mgr.GetActiveSessions()
	if finalCount != 0 {
		t.Errorf("Expected 0 sessions after cleanup, got %d", finalCount)
	}
}

func TestCleanupExpiredSessions_KeepsValid(t *testing.T) {
	mgr := NewManager("test-key", 1*time.Hour)
	
	// Create sessions
	for i := 0; i < 3; i++ {
		_, err := mgr.CreateSession("user"+string(rune(i)), "localhost")
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
	}
	
	// Cleanup (should not remove valid sessions)
	mgr.CleanupExpiredSessions()
	
	// Verify sessions still exist
	count := mgr.GetActiveSessions()
	if count != 3 {
		t.Errorf("Expected 3 sessions, got %d", count)
	}
}

func TestGetActiveSessions(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	// Initially should be 0
	if count := mgr.GetActiveSessions(); count != 0 {
		t.Errorf("Expected 0 sessions, got %d", count)
	}
	
	// Create sessions
	for i := 0; i < 10; i++ {
		_, err := mgr.CreateSession("user"+string(rune(i)), "localhost")
		if err != nil {
			t.Fatalf("Failed to create session: %v", err)
		}
	}
	
	// Should have 10 sessions
	if count := mgr.GetActiveSessions(); count != 10 {
		t.Errorf("Expected 10 sessions, got %d", count)
	}
}

func TestConcurrentSessionOperations(t *testing.T) {
	mgr := NewManager("test-key", 15*time.Minute)
	
	// Create sessions concurrently
	done := make(chan bool, 10)
	for i := 0; i < 10; i++ {
		go func(id int) {
			token, err := mgr.CreateSession("user"+string(rune(id)), "localhost")
			if err != nil {
				t.Errorf("Failed to create session: %v", err)
			}
			
			// Validate
			_, err = mgr.ValidateSession(token)
			if err != nil {
				t.Errorf("Failed to validate session: %v", err)
			}
			
			done <- true
		}(i)
	}
	
	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}
	
	// Verify count
	count := mgr.GetActiveSessions()
	if count != 10 {
		t.Errorf("Expected 10 sessions, got %d", count)
	}
}

func TestSessionWithDifferentSigningKeys(t *testing.T) {
	mgr1 := NewManager("key1", 15*time.Minute)
	mgr2 := NewManager("key2", 15*time.Minute)
	
	// Create session with mgr1
	token, err := mgr1.CreateSession("testuser", "localhost")
	if err != nil {
		t.Fatalf("Failed to create session: %v", err)
	}
	
	// Try to validate with mgr2 (different key)
	_, err = mgr2.ValidateSession(token)
	if err == nil {
		t.Error("Should not validate token with different signing key")
	}
}

func TestGenerateSessionID(t *testing.T) {
	// Generate multiple IDs
	ids := make(map[string]bool)
	for i := 0; i < 100; i++ {
		id := generateSessionID()
		
		if id == "" {
			t.Error("Generated empty session ID")
		}
		
		if ids[id] {
			t.Error("Generated duplicate session ID")
		}
		
		ids[id] = true
	}
}
