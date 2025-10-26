package mindb

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestNewAuditLogger_Disabled(t *testing.T) {
	logger, err := NewAuditLogger("", false)
	if err != nil {
		t.Fatalf("Failed to create disabled audit logger: %v", err)
	}
	
	if logger == nil {
		t.Fatal("NewAuditLogger returned nil")
	}
	
	if logger.enabled {
		t.Error("Logger should be disabled")
	}
}

func TestNewAuditLogger_Enabled(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create enabled audit logger: %v", err)
	}
	defer logger.Close()
	
	if !logger.enabled {
		t.Error("Logger should be enabled")
	}
	
	if logger.file == nil {
		t.Error("Log file should be open")
	}
	
	// Check that audit directory was created
	auditDir := filepath.Join(tmpDir, "audit")
	if _, err := os.Stat(auditDir); os.IsNotExist(err) {
		t.Error("Audit directory should be created")
	}
}

func TestAuditLogger_Log(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	event := AuditEvent{
		EventType: AuditLoginSuccess,
		Username:  "testuser",
		Host:      "localhost",
		Success:   true,
	}
	
	err = logger.Log(event)
	if err != nil {
		t.Fatalf("Failed to log event: %v", err)
	}
	
	// Verify log file was written
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	if len(files) == 0 {
		t.Fatal("No audit log files created")
	}
	
	// Read and verify log content
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var loggedEvent AuditEvent
	err = json.Unmarshal(content[:len(content)-1], &loggedEvent) // Remove trailing newline
	if err != nil {
		t.Fatalf("Failed to unmarshal log entry: %v", err)
	}
	
	if loggedEvent.EventType != AuditLoginSuccess {
		t.Errorf("Expected event type %s, got %s", AuditLoginSuccess, loggedEvent.EventType)
	}
	
	if loggedEvent.Username != "testuser" {
		t.Errorf("Expected username 'testuser', got '%s'", loggedEvent.Username)
	}
}

func TestAuditLogger_LogWhenDisabled(t *testing.T) {
	logger, err := NewAuditLogger("", false)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	
	event := AuditEvent{
		EventType: AuditLoginSuccess,
		Username:  "testuser",
		Host:      "localhost",
		Success:   true,
	}
	
	// Should not error when disabled
	err = logger.Log(event)
	if err != nil {
		t.Errorf("Log should not error when disabled: %v", err)
	}
}

func TestAuditLogger_LogLoginSuccess(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	logger.LogLoginSuccess("testuser", "localhost")
	
	// Verify log was written
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	if len(files) == 0 {
		t.Fatal("No audit log files created")
	}
}

func TestAuditLogger_LogLoginFailed(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	logger.LogLoginFailed("testuser", "localhost", "invalid password")
	
	// Read and verify
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var event AuditEvent
	json.Unmarshal(content[:len(content)-1], &event)
	
	if event.EventType != AuditLoginFailed {
		t.Errorf("Expected event type %s, got %s", AuditLoginFailed, event.EventType)
	}
	
	if event.Success {
		t.Error("Failed login should have Success=false")
	}
	
	if event.Details != "invalid password" {
		t.Errorf("Expected details 'invalid password', got '%s'", event.Details)
	}
}

func TestAuditLogger_LogUserCreated(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	logger.LogUserCreated("admin", "newuser", "localhost")
	
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var event AuditEvent
	json.Unmarshal(content[:len(content)-1], &event)
	
	if event.EventType != AuditUserCreated {
		t.Errorf("Expected event type %s, got %s", AuditUserCreated, event.EventType)
	}
	
	if event.Username != "admin" {
		t.Errorf("Expected username 'admin', got '%s'", event.Username)
	}
}

func TestAuditLogger_LogUserDropped(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	logger.LogUserDropped("admin", "olduser", "localhost")
	
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var event AuditEvent
	json.Unmarshal(content[:len(content)-1], &event)
	
	if event.EventType != AuditUserDropped {
		t.Errorf("Expected event type %s, got %s", AuditUserDropped, event.EventType)
	}
}

func TestAuditLogger_LogGrantPrivilege(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	privileges := []string{"SELECT", "INSERT"}
	logger.LogGrantPrivilege("admin", "testuser", "localhost", "testdb", "users", privileges)
	
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var event AuditEvent
	json.Unmarshal(content[:len(content)-1], &event)
	
	if event.EventType != AuditGrantPrivilege {
		t.Errorf("Expected event type %s, got %s", AuditGrantPrivilege, event.EventType)
	}
	
	if event.Database != "testdb" {
		t.Errorf("Expected database 'testdb', got '%s'", event.Database)
	}
}

func TestAuditLogger_LogRevokePrivilege(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	privileges := []string{"DELETE", "UPDATE"}
	logger.LogRevokePrivilege("admin", "testuser", "localhost", "testdb", "users", privileges)
	
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var event AuditEvent
	json.Unmarshal(content[:len(content)-1], &event)
	
	if event.EventType != AuditRevokePrivilege {
		t.Errorf("Expected event type %s, got %s", AuditRevokePrivilege, event.EventType)
	}
}

func TestAuditLogger_LogAccessDenied(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	logger.LogAccessDenied("testuser", "localhost", "testdb", "SELECT * FROM users", "insufficient privileges")
	
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var event AuditEvent
	json.Unmarshal(content[:len(content)-1], &event)
	
	if event.EventType != AuditAccessDenied {
		t.Errorf("Expected event type %s, got %s", AuditAccessDenied, event.EventType)
	}
	
	if event.Success {
		t.Error("Access denied should have Success=false")
	}
	
	if event.Query != "SELECT * FROM users" {
		t.Errorf("Expected query 'SELECT * FROM users', got '%s'", event.Query)
	}
}

func TestAuditLogger_LogQueryExecuted(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	logger.LogQueryExecuted("testuser", "localhost", "testdb", "SELECT * FROM users")
	
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var event AuditEvent
	json.Unmarshal(content[:len(content)-1], &event)
	
	if event.EventType != AuditQueryExecuted {
		t.Errorf("Expected event type %s, got %s", AuditQueryExecuted, event.EventType)
	}
	
	if !event.Success {
		t.Error("Query executed should have Success=true")
	}
}

func TestAuditLogger_Close(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	
	err = logger.Close()
	if err != nil {
		t.Errorf("Failed to close audit logger: %v", err)
	}
	
	// Should be able to close multiple times
	err = logger.Close()
	if err != nil {
		t.Errorf("Second close should not error: %v", err)
	}
}

func TestAuditLogger_Enable(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Start disabled
	logger, err := NewAuditLogger(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	if logger.enabled {
		t.Error("Logger should start disabled")
	}
	
	// Enable
	err = logger.Enable()
	if err != nil {
		t.Fatalf("Failed to enable logger: %v", err)
	}
	
	if !logger.enabled {
		t.Error("Logger should be enabled")
	}
	
	// Enable again (should be idempotent)
	err = logger.Enable()
	if err != nil {
		t.Errorf("Second enable should not error: %v", err)
	}
}

func TestAuditLogger_Disable(t *testing.T) {
	tmpDir := t.TempDir()
	
	// Start enabled
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	if !logger.enabled {
		t.Error("Logger should start enabled")
	}
	
	// Disable
	err = logger.Disable()
	if err != nil {
		t.Fatalf("Failed to disable logger: %v", err)
	}
	
	if logger.enabled {
		t.Error("Logger should be disabled")
	}
	
	// Disable again (should be idempotent)
	err = logger.Disable()
	if err != nil {
		t.Errorf("Second disable should not error: %v", err)
	}
}

func TestAuditLogger_EnableDisableCycle(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, false)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	// Enable -> Disable -> Enable
	logger.Enable()
	logger.LogLoginSuccess("user1", "host1")
	
	logger.Disable()
	logger.LogLoginSuccess("user2", "host2") // Should not log
	
	logger.Enable()
	logger.LogLoginSuccess("user3", "host3")
	
	// Verify logs
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	if len(files) == 0 {
		t.Fatal("No audit log files created")
	}
}

func TestAuditEvent_Timestamp(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	// Log without timestamp
	event := AuditEvent{
		EventType: AuditLoginSuccess,
		Username:  "testuser",
		Host:      "localhost",
		Success:   true,
	}
	
	logger.Log(event)
	
	// Read and verify timestamp was set
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	var loggedEvent AuditEvent
	json.Unmarshal(content[:len(content)-1], &loggedEvent)
	
	if loggedEvent.Timestamp.IsZero() {
		t.Error("Timestamp should be set automatically")
	}
	
	// Should be recent
	if time.Since(loggedEvent.Timestamp) > time.Minute {
		t.Error("Timestamp should be recent")
	}
}

func TestAuditLogger_MultipleEvents(t *testing.T) {
	tmpDir := t.TempDir()
	
	logger, err := NewAuditLogger(tmpDir, true)
	if err != nil {
		t.Fatalf("Failed to create audit logger: %v", err)
	}
	defer logger.Close()
	
	// Log multiple events
	logger.LogLoginSuccess("user1", "host1")
	logger.LogLoginFailed("user2", "host2", "bad password")
	logger.LogUserCreated("admin", "user3", "host3")
	logger.LogQueryExecuted("user1", "host1", "testdb", "SELECT 1")
	
	// Verify all events were logged
	auditDir := filepath.Join(tmpDir, "audit")
	files, err := os.ReadDir(auditDir)
	if err != nil {
		t.Fatalf("Failed to read audit directory: %v", err)
	}
	
	logFile := filepath.Join(auditDir, files[0].Name())
	content, err := os.ReadFile(logFile)
	if err != nil {
		t.Fatalf("Failed to read log file: %v", err)
	}
	
	// Count lines (events)
	lines := 0
	for _, b := range content {
		if b == '\n' {
			lines++
		}
	}
	
	if lines != 4 {
		t.Errorf("Expected 4 log entries, got %d", lines)
	}
}

func TestAuditEventTypes(t *testing.T) {
	types := []AuditEventType{
		AuditLoginSuccess,
		AuditLoginFailed,
		AuditLogout,
		AuditUserCreated,
		AuditUserDropped,
		AuditGrantPrivilege,
		AuditRevokePrivilege,
		AuditAccessDenied,
		AuditQueryExecuted,
	}
	
	for _, eventType := range types {
		if string(eventType) == "" {
			t.Errorf("Event type should not be empty")
		}
	}
}
