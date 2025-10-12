package session

import (
	"crypto/rand"
	"encoding/base64"
	"errors"
	"sync"
	"time"

	"github.com/golang-jwt/jwt/v5"
)

var (
	ErrInvalidToken   = errors.New("invalid token")
	ErrExpiredToken   = errors.New("token expired")
	ErrSessionExpired = errors.New("session expired")
)

// Claims represents the JWT claims for a session
type Claims struct {
	Username string `json:"username"`
	Host     string `json:"host"`
	jwt.RegisteredClaims
}

// Manager handles session creation and validation
type Manager struct {
	signingKey     []byte
	sessionTimeout time.Duration
	sessions       map[string]*SessionInfo
	mu             sync.RWMutex
}

// SessionInfo stores session metadata
type SessionInfo struct {
	Username   string
	Host       string
	CreatedAt  time.Time
	LastAccess time.Time
	ExpiresAt  time.Time
}

// NewManager creates a new session manager
func NewManager(signingKey string, sessionTimeout time.Duration) *Manager {
	key := []byte(signingKey)
	if len(key) == 0 {
		// Generate random signing key if not provided
		key = make([]byte, 32)
		rand.Read(key)
	}

	return &Manager{
		signingKey:     key,
		sessionTimeout: sessionTimeout,
		sessions:       make(map[string]*SessionInfo),
	}
}

// CreateSession creates a new session and returns a JWT token
func (m *Manager) CreateSession(username, host string) (string, error) {
	now := time.Now()
	expiresAt := now.Add(m.sessionTimeout)

	// Create JWT claims
	claims := &Claims{
		Username: username,
		Host:     host,
		RegisteredClaims: jwt.RegisteredClaims{
			ExpiresAt: jwt.NewNumericDate(expiresAt),
			IssuedAt:  jwt.NewNumericDate(now),
			NotBefore: jwt.NewNumericDate(now),
			ID:        generateSessionID(),
		},
	}

	// Create token
	token := jwt.NewWithClaims(jwt.SigningMethodHS256, claims)
	tokenString, err := token.SignedString(m.signingKey)
	if err != nil {
		return "", err
	}

	// Store session info
	m.mu.Lock()
	m.sessions[claims.ID] = &SessionInfo{
		Username:   username,
		Host:       host,
		CreatedAt:  now,
		LastAccess: now,
		ExpiresAt:  expiresAt,
	}
	m.mu.Unlock()

	return tokenString, nil
}

// ValidateSession validates a JWT token and updates last access time
func (m *Manager) ValidateSession(tokenString string) (*Claims, error) {
	// Parse token
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		// Verify signing method
		if _, ok := token.Method.(*jwt.SigningMethodHMAC); !ok {
			return nil, ErrInvalidToken
		}
		return m.signingKey, nil
	})

	if err != nil {
		return nil, err
	}

	// Extract claims
	claims, ok := token.Claims.(*Claims)
	if !ok || !token.Valid {
		return nil, ErrInvalidToken
	}

	// Check if session exists and is valid
	m.mu.Lock()
	defer m.mu.Unlock()

	sessionInfo, exists := m.sessions[claims.ID]
	if !exists {
		return nil, ErrSessionExpired
	}

	// Check if session has expired
	if time.Now().After(sessionInfo.ExpiresAt) {
		delete(m.sessions, claims.ID)
		return nil, ErrExpiredToken
	}

	// Update last access time
	sessionInfo.LastAccess = time.Now()

	return claims, nil
}

// RefreshSession extends the session expiration time
func (m *Manager) RefreshSession(tokenString string) (string, error) {
	// Validate current session
	claims, err := m.ValidateSession(tokenString)
	if err != nil {
		return "", err
	}

	// Create new session with extended expiration
	return m.CreateSession(claims.Username, claims.Host)
}

// RevokeSession removes a session
func (m *Manager) RevokeSession(tokenString string) error {
	// Parse token to get session ID
	token, err := jwt.ParseWithClaims(tokenString, &Claims{}, func(token *jwt.Token) (interface{}, error) {
		return m.signingKey, nil
	})

	if err != nil {
		return err
	}

	claims, ok := token.Claims.(*Claims)
	if !ok {
		return ErrInvalidToken
	}

	// Remove session
	m.mu.Lock()
	delete(m.sessions, claims.ID)
	m.mu.Unlock()

	return nil
}

// CleanupExpiredSessions removes expired sessions (should be called periodically)
func (m *Manager) CleanupExpiredSessions() {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	for id, session := range m.sessions {
		if now.After(session.ExpiresAt) {
			delete(m.sessions, id)
		}
	}
}

// GetActiveSessions returns the number of active sessions
func (m *Manager) GetActiveSessions() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.sessions)
}

// generateSessionID generates a random session ID
func generateSessionID() string {
	b := make([]byte, 32)
	rand.Read(b)
	return base64.URLEncoding.EncodeToString(b)
}
