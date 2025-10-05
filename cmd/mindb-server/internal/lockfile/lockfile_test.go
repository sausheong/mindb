package lockfile

import (
	"os"
	"path/filepath"
	"testing"
)

func TestAcquire(t *testing.T) {
	tmpDir := t.TempDir()

	// Acquire lock
	lock, err := Acquire(tmpDir)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	defer lock.Release()

	// Verify lock file exists
	lockPath := filepath.Join(tmpDir, lockFileName)
	if _, err := os.Stat(lockPath); os.IsNotExist(err) {
		t.Error("Lock file does not exist")
	}

	// Verify lock file contains PID
	content, err := os.ReadFile(lockPath)
	if err != nil {
		t.Fatalf("Failed to read lock file: %v", err)
	}

	if len(content) == 0 {
		t.Error("Lock file is empty")
	}
}

func TestAcquire_AlreadyLocked(t *testing.T) {
	tmpDir := t.TempDir()

	// Acquire first lock
	lock1, err := Acquire(tmpDir)
	if err != nil {
		t.Fatalf("Failed to acquire first lock: %v", err)
	}
	defer lock1.Release()

	// Try to acquire second lock (should fail)
	lock2, err := Acquire(tmpDir)
	if err == nil {
		lock2.Release()
		t.Error("Expected error when acquiring already-locked directory")
	}
}

func TestRelease(t *testing.T) {
	tmpDir := t.TempDir()

	// Acquire lock
	lock, err := Acquire(tmpDir)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}

	// Release lock
	if err := lock.Release(); err != nil {
		t.Fatalf("Failed to release lock: %v", err)
	}

	// Verify lock file is removed
	lockPath := filepath.Join(tmpDir, lockFileName)
	if _, err := os.Stat(lockPath); !os.IsNotExist(err) {
		t.Error("Lock file still exists after release")
	}

	// Should be able to acquire again
	lock2, err := Acquire(tmpDir)
	if err != nil {
		t.Fatalf("Failed to acquire lock after release: %v", err)
	}
	defer lock2.Release()
}

func TestParseLockFile(t *testing.T) {
	tmpDir := t.TempDir()

	// Acquire lock
	lock, err := Acquire(tmpDir)
	if err != nil {
		t.Fatalf("Failed to acquire lock: %v", err)
	}
	defer lock.Release()

	// Parse lock file
	lockPath := filepath.Join(tmpDir, lockFileName)
	pid, hostname, startTime, err := ParseLockFile(lockPath)
	if err != nil {
		t.Fatalf("Failed to parse lock file: %v", err)
	}

	// Verify PID
	if pid != os.Getpid() {
		t.Errorf("Expected PID %d, got %d", os.Getpid(), pid)
	}

	// Verify hostname is not empty
	if hostname == "" {
		t.Error("Hostname is empty")
	}

	// Verify start time is not empty
	if startTime == "" {
		t.Error("Start time is empty")
	}
}

func TestIsProcessAlive(t *testing.T) {
	// Current process should be alive
	if !IsProcessAlive(os.Getpid()) {
		t.Error("Current process should be alive")
	}

	// PID 99999 should not exist
	if IsProcessAlive(99999) {
		t.Error("PID 99999 should not exist")
	}
}
