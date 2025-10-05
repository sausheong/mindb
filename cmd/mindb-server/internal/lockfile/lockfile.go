package lockfile

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

const lockFileName = ".lock"

// Lock represents an exclusive lock on a directory
type Lock struct {
	path string
	file *os.File
}

// Acquire creates an exclusive lock on the given directory
// Returns error if another process holds the lock
func Acquire(dataDir string) (*Lock, error) {
	if err := os.MkdirAll(dataDir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create data directory: %w", err)
	}

	lockPath := filepath.Join(dataDir, lockFileName)
	
	// Try to open/create the lock file
	file, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open lock file: %w", err)
	}

	// Try to acquire exclusive lock
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		file.Close()
		
		// Read existing lock info
		content, _ := os.ReadFile(lockPath)
		existingInfo := string(content)
		
		return nil, fmt.Errorf("data directory is locked by another process: %s\nLock info: %s", 
			dataDir, existingInfo)
	}

	// Write lock information
	pid := os.Getpid()
	startTime := time.Now().Format(time.RFC3339)
	hostname, _ := os.Hostname()
	
	lockInfo := fmt.Sprintf("PID: %d\nHostname: %s\nStarted: %s\n", pid, hostname, startTime)
	
	if err := file.Truncate(0); err != nil {
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
		return nil, fmt.Errorf("failed to write lock info: %w", err)
	}
	
	if _, err := file.WriteAt([]byte(lockInfo), 0); err != nil {
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
		return nil, fmt.Errorf("failed to write lock info: %w", err)
	}
	
	if err := file.Sync(); err != nil {
		syscall.Flock(int(file.Fd()), syscall.LOCK_UN)
		file.Close()
		return nil, fmt.Errorf("failed to sync lock file: %w", err)
	}

	return &Lock{
		path: lockPath,
		file: file,
	}, nil
}

// Release removes the lock
func (l *Lock) Release() error {
	if l.file == nil {
		return nil
	}

	// Release the flock
	if err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN); err != nil {
		return fmt.Errorf("failed to release lock: %w", err)
	}

	// Close the file
	if err := l.file.Close(); err != nil {
		return fmt.Errorf("failed to close lock file: %w", err)
	}

	// Remove the lock file
	if err := os.Remove(l.path); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("failed to remove lock file: %w", err)
	}

	l.file = nil
	return nil
}

// IsProcessAlive checks if a process with the given PID is running
func IsProcessAlive(pid int) bool {
	process, err := os.FindProcess(pid)
	if err != nil {
		return false
	}
	
	// Send signal 0 to check if process exists
	err = process.Signal(syscall.Signal(0))
	return err == nil
}

// ParseLockFile reads and parses the lock file
func ParseLockFile(lockPath string) (pid int, hostname, startTime string, err error) {
	content, err := os.ReadFile(lockPath)
	if err != nil {
		return 0, "", "", err
	}

	lines := strings.Split(string(content), "\n")
	for _, line := range lines {
		parts := strings.SplitN(line, ": ", 2)
		if len(parts) != 2 {
			continue
		}
		
		key, value := parts[0], parts[1]
		switch key {
		case "PID":
			pid, _ = strconv.Atoi(value)
		case "Hostname":
			hostname = value
		case "Started":
			startTime = value
		}
	}
	
	return pid, hostname, startTime, nil
}
