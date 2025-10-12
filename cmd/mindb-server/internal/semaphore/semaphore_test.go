package semaphore

import (
	"context"
	"sync"
	"testing"
	"time"
)

func TestNew(t *testing.T) {
	sem := New(5)
	if sem == nil {
		t.Fatal("New returned nil")
	}
	if sem.sem == nil {
		t.Error("sem channel is nil")
	}
	if cap(sem.sem) != 5 {
		t.Errorf("Expected capacity 5, got %d", cap(sem.sem))
	}
}

func TestAcquire_Success(t *testing.T) {
	sem := New(2)
	ctx := context.Background()

	// First acquire should succeed
	err := sem.Acquire(ctx)
	if err != nil {
		t.Errorf("First acquire failed: %v", err)
	}

	// Second acquire should succeed
	err = sem.Acquire(ctx)
	if err != nil {
		t.Errorf("Second acquire failed: %v", err)
	}

	// Check that 2 slots are in use
	if sem.InUse() != 2 {
		t.Errorf("Expected 2 slots in use, got %d", sem.InUse())
	}
	if sem.Available() != 0 {
		t.Errorf("Expected 0 slots available, got %d", sem.Available())
	}
}

func TestAcquire_ContextCancelled(t *testing.T) {
	sem := New(1)

	// Acquire the only slot
	ctx := context.Background()
	sem.Acquire(ctx)

	// Try to acquire with cancelled context
	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately

	err := sem.Acquire(cancelledCtx)
	if err == nil {
		t.Error("Expected error for cancelled context")
	}
}

func TestAcquire_ContextTimeout(t *testing.T) {
	sem := New(1)

	// Acquire the only slot
	ctx := context.Background()
	sem.Acquire(ctx)

	// Try to acquire with timeout context
	timeoutCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()

	err := sem.Acquire(timeoutCtx)
	if err == nil {
		t.Error("Expected error for timed-out context")
	}
}

func TestTryAcquire_Success(t *testing.T) {
	sem := New(2)

	// First try should succeed
	if !sem.TryAcquire() {
		t.Error("First TryAcquire should succeed")
	}

	// Second try should succeed
	if !sem.TryAcquire() {
		t.Error("Second TryAcquire should succeed")
	}

	// Check that 2 slots are in use
	if sem.InUse() != 2 {
		t.Errorf("Expected 2 slots in use, got %d", sem.InUse())
	}
}

func TestTryAcquire_Failure(t *testing.T) {
	sem := New(1)

	// Acquire the only slot
	sem.TryAcquire()

	// Next try should fail
	if sem.TryAcquire() {
		t.Error("TryAcquire should fail when full")
	}
}

func TestRelease(t *testing.T) {
	sem := New(2)

	// Acquire both slots
	sem.Acquire(context.Background())
	sem.Acquire(context.Background())

	if sem.Available() != 0 {
		t.Errorf("Expected 0 available, got %d", sem.Available())
	}

	// Release one slot
	sem.Release()

	if sem.Available() != 1 {
		t.Errorf("Expected 1 available after release, got %d", sem.Available())
	}
	if sem.InUse() != 1 {
		t.Errorf("Expected 1 in use after release, got %d", sem.InUse())
	}

	// Release second slot
	sem.Release()

	if sem.Available() != 2 {
		t.Errorf("Expected 2 available after second release, got %d", sem.Available())
	}
	if sem.InUse() != 0 {
		t.Errorf("Expected 0 in use after second release, got %d", sem.InUse())
	}
}

func TestRelease_ExtraRelease(t *testing.T) {
	sem := New(1)

	// Release without acquiring (should not panic)
	sem.Release()

	// Should still work normally
	if !sem.TryAcquire() {
		t.Error("TryAcquire should work after extra release")
	}
}

func TestAvailable(t *testing.T) {
	sem := New(5)

	if sem.Available() != 5 {
		t.Errorf("Expected 5 available initially, got %d", sem.Available())
	}

	sem.Acquire(context.Background())
	if sem.Available() != 4 {
		t.Errorf("Expected 4 available after one acquire, got %d", sem.Available())
	}

	sem.Acquire(context.Background())
	if sem.Available() != 3 {
		t.Errorf("Expected 3 available after two acquires, got %d", sem.Available())
	}
}

func TestInUse(t *testing.T) {
	sem := New(5)

	if sem.InUse() != 0 {
		t.Errorf("Expected 0 in use initially, got %d", sem.InUse())
	}

	sem.Acquire(context.Background())
	if sem.InUse() != 1 {
		t.Errorf("Expected 1 in use after one acquire, got %d", sem.InUse())
	}

	sem.Acquire(context.Background())
	if sem.InUse() != 2 {
		t.Errorf("Expected 2 in use after two acquires, got %d", sem.InUse())
	}
}

func TestConcurrentAcquireRelease(t *testing.T) {
	sem := New(3)
	iterations := 100
	var wg sync.WaitGroup

	// Launch multiple goroutines that acquire and release
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < iterations; j++ {
				ctx := context.Background()
				if err := sem.Acquire(ctx); err != nil {
					t.Errorf("Acquire failed: %v", err)
					return
				}
				// Simulate some work
				time.Sleep(time.Microsecond)
				sem.Release()
			}
		}()
	}

	wg.Wait()

	// All slots should be released
	if sem.InUse() != 0 {
		t.Errorf("Expected 0 in use after all releases, got %d", sem.InUse())
	}
	if sem.Available() != 3 {
		t.Errorf("Expected 3 available after all releases, got %d", sem.Available())
	}
}

func TestConcurrentTryAcquire(t *testing.T) {
	sem := New(5)
	successCount := 0
	var mu sync.Mutex
	var wg sync.WaitGroup

	// Launch multiple goroutines trying to acquire
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if sem.TryAcquire() {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	wg.Wait()

	// Only 5 should succeed (capacity is 5)
	if successCount != 5 {
		t.Errorf("Expected 5 successful acquires, got %d", successCount)
	}
	if sem.InUse() != 5 {
		t.Errorf("Expected 5 in use, got %d", sem.InUse())
	}
}

func TestBlockingBehavior(t *testing.T) {
	sem := New(1)
	ctx := context.Background()

	// Acquire the only slot
	sem.Acquire(ctx)

	// Try to acquire in a goroutine (should block)
	acquired := make(chan bool, 1)
	go func() {
		err := sem.Acquire(ctx)
		if err == nil {
			acquired <- true
		}
	}()

	// Wait a bit to ensure goroutine is blocked
	time.Sleep(50 * time.Millisecond)

	select {
	case <-acquired:
		t.Error("Acquire should have blocked")
	default:
		// Expected - still blocked
	}

	// Release the slot
	sem.Release()

	// Now the blocked acquire should succeed
	select {
	case <-acquired:
		// Expected
	case <-time.After(100 * time.Millisecond):
		t.Error("Acquire should have succeeded after release")
	}
}

func TestZeroCapacity(t *testing.T) {
	sem := New(0)

	// TryAcquire should always fail
	if sem.TryAcquire() {
		t.Error("TryAcquire should fail with zero capacity")
	}

	// Available should be 0
	if sem.Available() != 0 {
		t.Errorf("Expected 0 available, got %d", sem.Available())
	}
}

func TestLargeCapacity(t *testing.T) {
	sem := New(1000)

	// Acquire many slots
	for i := 0; i < 500; i++ {
		if err := sem.Acquire(context.Background()); err != nil {
			t.Errorf("Acquire %d failed: %v", i, err)
		}
	}

	if sem.InUse() != 500 {
		t.Errorf("Expected 500 in use, got %d", sem.InUse())
	}
	if sem.Available() != 500 {
		t.Errorf("Expected 500 available, got %d", sem.Available())
	}
}
