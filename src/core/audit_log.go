package mindb

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// AuditEventType represents the type of audit event
type AuditEventType string

const (
	AuditLoginSuccess    AuditEventType = "LOGIN_SUCCESS"
	AuditLoginFailed     AuditEventType = "LOGIN_FAILED"
	AuditLogout          AuditEventType = "LOGOUT"
	AuditUserCreated     AuditEventType = "USER_CREATED"
	AuditUserDropped     AuditEventType = "USER_DROPPED"
	AuditGrantPrivilege  AuditEventType = "GRANT_PRIVILEGE"
	AuditRevokePrivilege AuditEventType = "REVOKE_PRIVILEGE"
	AuditAccessDenied    AuditEventType = "ACCESS_DENIED"
	AuditQueryExecuted   AuditEventType = "QUERY_EXECUTED"
)

// AuditEvent represents a single audit log entry
type AuditEvent struct {
	Timestamp time.Time      `json:"timestamp"`
	EventType AuditEventType `json:"event_type"`
	Username  string         `json:"username"`
	Host      string         `json:"host"`
	Database  string         `json:"database,omitempty"`
	Query     string         `json:"query,omitempty"`
	Details   string         `json:"details,omitempty"`
	Success   bool           `json:"success"`
}

// AuditLogger manages audit logging
type AuditLogger struct {
	dataDir  string
	file     *os.File
	enabled  bool
	mu       sync.Mutex
}

// NewAuditLogger creates a new audit logger
func NewAuditLogger(dataDir string, enabled bool) (*AuditLogger, error) {
	logger := &AuditLogger{
		dataDir: dataDir,
		enabled: enabled,
	}
	
	if enabled && dataDir != "" {
		if err := logger.openLogFile(); err != nil {
			return nil, err
		}
	}
	
	return logger, nil
}

// openLogFile opens the audit log file
func (al *AuditLogger) openLogFile() error {
	// Create audit directory
	auditDir := filepath.Join(al.dataDir, "audit")
	if err := os.MkdirAll(auditDir, 0755); err != nil {
		return fmt.Errorf("failed to create audit directory: %w", err)
	}
	
	// Create log file with date
	filename := fmt.Sprintf("audit-%s.log", time.Now().Format("2006-01-02"))
	filePath := filepath.Join(auditDir, filename)
	
	// Open file in append mode
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return fmt.Errorf("failed to open audit log file: %w", err)
	}
	
	al.file = file
	return nil
}

// Log logs an audit event
func (al *AuditLogger) Log(event AuditEvent) error {
	if !al.enabled {
		return nil
	}
	
	al.mu.Lock()
	defer al.mu.Unlock()
	
	// Set timestamp if not set
	if event.Timestamp.IsZero() {
		event.Timestamp = time.Now()
	}
	
	// Marshal to JSON
	jsonData, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("failed to marshal audit event: %w", err)
	}
	
	// Write to file
	if al.file != nil {
		if _, err := al.file.Write(append(jsonData, '\n')); err != nil {
			return fmt.Errorf("failed to write audit log: %w", err)
		}
		
		// Flush to disk
		if err := al.file.Sync(); err != nil {
			return fmt.Errorf("failed to sync audit log: %w", err)
		}
	}
	
	return nil
}

// LogLoginSuccess logs a successful login
func (al *AuditLogger) LogLoginSuccess(username, host string) {
	al.Log(AuditEvent{
		EventType: AuditLoginSuccess,
		Username:  username,
		Host:      host,
		Success:   true,
	})
}

// LogLoginFailed logs a failed login attempt
func (al *AuditLogger) LogLoginFailed(username, host, reason string) {
	al.Log(AuditEvent{
		EventType: AuditLoginFailed,
		Username:  username,
		Host:      host,
		Details:   reason,
		Success:   false,
	})
}

// LogUserCreated logs user creation
func (al *AuditLogger) LogUserCreated(admin, username, host string) {
	al.Log(AuditEvent{
		EventType: AuditUserCreated,
		Username:  admin,
		Details:   fmt.Sprintf("Created user '%s'@'%s'", username, host),
		Success:   true,
	})
}

// LogUserDropped logs user deletion
func (al *AuditLogger) LogUserDropped(admin, username, host string) {
	al.Log(AuditEvent{
		EventType: AuditUserDropped,
		Username:  admin,
		Details:   fmt.Sprintf("Dropped user '%s'@'%s'", username, host),
		Success:   true,
	})
}

// LogGrantPrivilege logs privilege grant
func (al *AuditLogger) LogGrantPrivilege(admin, username, host, database, table string, privileges []string) {
	al.Log(AuditEvent{
		EventType: AuditGrantPrivilege,
		Username:  admin,
		Database:  database,
		Details:   fmt.Sprintf("Granted %v on %s.%s to '%s'@'%s'", privileges, database, table, username, host),
		Success:   true,
	})
}

// LogRevokePrivilege logs privilege revocation
func (al *AuditLogger) LogRevokePrivilege(admin, username, host, database, table string, privileges []string) {
	al.Log(AuditEvent{
		EventType: AuditRevokePrivilege,
		Username:  admin,
		Database:  database,
		Details:   fmt.Sprintf("Revoked %v on %s.%s from '%s'@'%s'", privileges, database, table, username, host),
		Success:   true,
	})
}

// LogAccessDenied logs access denied events
func (al *AuditLogger) LogAccessDenied(username, host, database, query, reason string) {
	al.Log(AuditEvent{
		EventType: AuditAccessDenied,
		Username:  username,
		Host:      host,
		Database:  database,
		Query:     query,
		Details:   reason,
		Success:   false,
	})
}

// LogQueryExecuted logs successful query execution
func (al *AuditLogger) LogQueryExecuted(username, host, database, query string) {
	al.Log(AuditEvent{
		EventType: AuditQueryExecuted,
		Username:  username,
		Host:      host,
		Database:  database,
		Query:     query,
		Success:   true,
	})
}

// Close closes the audit log file
func (al *AuditLogger) Close() error {
	al.mu.Lock()
	defer al.mu.Unlock()
	
	if al.file != nil {
		err := al.file.Close()
		al.file = nil // Prevent double-close
		return err
	}
	
	return nil
}

// Enable enables audit logging
func (al *AuditLogger) Enable() error {
	al.mu.Lock()
	defer al.mu.Unlock()
	
	if al.enabled {
		return nil
	}
	
	al.enabled = true
	
	if al.dataDir != "" {
		return al.openLogFile()
	}
	
	return nil
}

// Disable disables audit logging
func (al *AuditLogger) Disable() error {
	al.mu.Lock()
	defer al.mu.Unlock()
	
	al.enabled = false
	
	if al.file != nil {
		if err := al.file.Close(); err != nil {
			return err
		}
		al.file = nil
	}
	
	return nil
}
