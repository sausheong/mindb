package txmanager

import (
	"context"
	"testing"
	"time"
)

func TestNewManager(t *testing.T) {
	mgr := NewManager(1*time.Second, 10, 5)
	if mgr == nil {
		t.Fatal("Expected non-nil manager")
	}
	defer mgr.Close()

	if mgr.idleTimeout != 1*time.Second {
		t.Errorf("Expected idle timeout 1s, got %v", mgr.idleTimeout)
	}
}

func TestBegin(t *testing.T) {
	mgr := NewManager(1*time.Minute, 10, 5)
	defer mgr.Close()

	ctx := context.Background()
	tx, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	if tx.ID == "" {
		t.Error("Transaction ID is empty")
	}

	if tx.ClientID != "client1" {
		t.Errorf("Expected client1, got %s", tx.ClientID)
	}
}

func TestBegin_MaxOpenTx(t *testing.T) {
	mgr := NewManager(1*time.Minute, 2, 5)
	defer mgr.Close()

	ctx := context.Background()

	// Create 2 transactions (max)
	tx1, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin 1 failed: %v", err)
	}

	tx2, err := mgr.Begin(ctx, "client2", nil)
	if err != nil {
		t.Fatalf("Begin 2 failed: %v", err)
	}

	// Third should fail
	_, err = mgr.Begin(ctx, "client3", nil)
	if err == nil {
		t.Error("Expected error when exceeding max open transactions")
	}

	// Remove one and try again
	mgr.Remove(tx1.ID)

	tx3, err := mgr.Begin(ctx, "client3", nil)
	if err != nil {
		t.Fatalf("Begin 3 failed after removal: %v", err)
	}

	mgr.Remove(tx2.ID)
	mgr.Remove(tx3.ID)
}

func TestBegin_MaxPerClient(t *testing.T) {
	mgr := NewManager(1*time.Minute, 10, 2)
	defer mgr.Close()

	ctx := context.Background()

	// Create 2 transactions for same client (max)
	tx1, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin 1 failed: %v", err)
	}

	tx2, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin 2 failed: %v", err)
	}

	// Third for same client should fail
	_, err = mgr.Begin(ctx, "client1", nil)
	if err == nil {
		t.Error("Expected error when exceeding max transactions per client")
	}

	// Different client should succeed
	tx3, err := mgr.Begin(ctx, "client2", nil)
	if err != nil {
		t.Fatalf("Begin for client2 failed: %v", err)
	}

	mgr.Remove(tx1.ID)
	mgr.Remove(tx2.ID)
	mgr.Remove(tx3.ID)
}

func TestGet(t *testing.T) {
	mgr := NewManager(1*time.Minute, 10, 5)
	defer mgr.Close()

	ctx := context.Background()
	tx, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Get transaction
	retrieved, err := mgr.Get(tx.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if retrieved.ID != tx.ID {
		t.Errorf("Expected ID %s, got %s", tx.ID, retrieved.ID)
	}

	// Get non-existent transaction
	_, err = mgr.Get("nonexistent")
	if err == nil {
		t.Error("Expected error for non-existent transaction")
	}
}

func TestTouch(t *testing.T) {
	mgr := NewManager(1*time.Minute, 10, 5)
	defer mgr.Close()

	ctx := context.Background()
	tx, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	originalTime := tx.LastUsedAt
	time.Sleep(10 * time.Millisecond)

	// Touch transaction
	if err := mgr.Touch(tx.ID); err != nil {
		t.Fatalf("Touch failed: %v", err)
	}

	// Get updated transaction
	updated, err := mgr.Get(tx.ID)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}

	if !updated.LastUsedAt.After(originalTime) {
		t.Error("LastUsedAt was not updated")
	}
}

func TestRemove(t *testing.T) {
	mgr := NewManager(1*time.Minute, 10, 5)
	defer mgr.Close()

	ctx := context.Background()
	tx, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Remove transaction
	if err := mgr.Remove(tx.ID); err != nil {
		t.Fatalf("Remove failed: %v", err)
	}

	// Should not be found
	_, err = mgr.Get(tx.ID)
	if err == nil {
		t.Error("Expected error after removal")
	}

	// Double remove should not error
	if err := mgr.Remove(tx.ID); err != nil {
		t.Errorf("Double remove should not error: %v", err)
	}
}

func TestStats(t *testing.T) {
	mgr := NewManager(1*time.Minute, 10, 5)
	defer mgr.Close()

	ctx := context.Background()

	// Initially empty
	total, clients := mgr.Stats()
	if total != 0 || clients != 0 {
		t.Errorf("Expected 0 transactions, got %d total, %d clients", total, clients)
	}

	// Add transactions
	tx1, _ := mgr.Begin(ctx, "client1", nil)
	tx2, _ := mgr.Begin(ctx, "client1", nil)
	tx3, _ := mgr.Begin(ctx, "client2", nil)

	total, clients = mgr.Stats()
	if total != 3 {
		t.Errorf("Expected 3 transactions, got %d", total)
	}
	if clients != 2 {
		t.Errorf("Expected 2 clients, got %d", clients)
	}

	// Remove all
	mgr.Remove(tx1.ID)
	mgr.Remove(tx2.ID)
	mgr.Remove(tx3.ID)

	total, clients = mgr.Stats()
	if total != 0 || clients != 0 {
		t.Errorf("Expected 0 after removal, got %d total, %d clients", total, clients)
	}
}

func TestCleanupIdle(t *testing.T) {
	mgr := NewManager(100*time.Millisecond, 10, 5)
	defer mgr.Close()

	ctx := context.Background()
	tx, err := mgr.Begin(ctx, "client1", nil)
	if err != nil {
		t.Fatalf("Begin failed: %v", err)
	}

	// Wait for idle timeout
	time.Sleep(150 * time.Millisecond)

	// Transaction should be cleaned up
	_, err = mgr.Get(tx.ID)
	if err == nil {
		t.Error("Expected transaction to be cleaned up")
	}
}

func TestGenerateTxID(t *testing.T) {
	id1, err := generateTxID()
	if err != nil {
		t.Fatalf("generateTxID failed: %v", err)
	}

	if len(id1) != 32 { // 16 bytes = 32 hex chars
		t.Errorf("Expected 32 char ID, got %d", len(id1))
	}

	// Generate another - should be different
	id2, err := generateTxID()
	if err != nil {
		t.Fatalf("generateTxID failed: %v", err)
	}

	if id1 == id2 {
		t.Error("Expected unique IDs")
	}
}
