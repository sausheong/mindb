package semaphore

import (
	"context"
	"fmt"
)

// Semaphore provides bounded concurrency control
type Semaphore struct {
	sem chan struct{}
}

// New creates a new semaphore with the given capacity
func New(capacity int) *Semaphore {
	return &Semaphore{
		sem: make(chan struct{}, capacity),
	}
}

// Acquire acquires a slot, blocking until one is available or context is cancelled
func (s *Semaphore) Acquire(ctx context.Context) error {
	select {
	case s.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("semaphore acquire cancelled: %w", ctx.Err())
	}
}

// TryAcquire attempts to acquire a slot without blocking
func (s *Semaphore) TryAcquire() bool {
	select {
	case s.sem <- struct{}{}:
		return true
	default:
		return false
	}
}

// Release releases a slot
func (s *Semaphore) Release() {
	select {
	case <-s.sem:
	default:
		// Should not happen if used correctly
	}
}

// Available returns the number of available slots
func (s *Semaphore) Available() int {
	return cap(s.sem) - len(s.sem)
}

// InUse returns the number of slots in use
func (s *Semaphore) InUse() int {
	return len(s.sem)
}
