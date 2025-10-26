package txmanager

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync"
	"time"
)

// Transaction represents an active transaction
type Transaction struct {
	ID         string
	ClientID   string
	CreatedAt  time.Time
	LastUsedAt time.Time
	Context    context.Context
	Cancel     context.CancelFunc
	
	// Mindb transaction handle (interface{} to avoid circular dependency)
	Handle interface{}
}

// Manager manages active transactions with TTL
type Manager struct {
	mu           sync.RWMutex
	transactions map[string]*Transaction
	byClient     map[string][]string // clientID -> []txID
	idleTimeout  time.Duration
	maxOpenTx    int
	maxPerClient int
	stopCh       chan struct{}
	wg           sync.WaitGroup
}

// NewManager creates a new transaction manager
func NewManager(idleTimeout time.Duration, maxOpenTx, maxPerClient int) *Manager {
	m := &Manager{
		transactions: make(map[string]*Transaction),
		byClient:     make(map[string][]string),
		idleTimeout:  idleTimeout,
		maxOpenTx:    maxOpenTx,
		maxPerClient: maxPerClient,
		stopCh:       make(chan struct{}),
	}
	
	// Start cleanup goroutine
	m.wg.Add(1)
	go m.cleanupLoop()
	
	return m
}

// Begin creates a new transaction
func (m *Manager) Begin(ctx context.Context, clientID string, handle interface{}) (*Transaction, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Check global limit
	if len(m.transactions) >= m.maxOpenTx {
		return nil, fmt.Errorf("max open transactions reached (%d)", m.maxOpenTx)
	}
	
	// Check per-client limit
	clientTxs := m.byClient[clientID]
	if len(clientTxs) >= m.maxPerClient {
		return nil, fmt.Errorf("max transactions per client reached (%d)", m.maxPerClient)
	}
	
	// Generate transaction ID
	txID, err := generateTxID()
	if err != nil {
		return nil, fmt.Errorf("failed to generate transaction ID: %w", err)
	}
	
	// Create transaction with cancellable context
	txCtx, cancel := context.WithCancel(ctx)
	
	tx := &Transaction{
		ID:         txID,
		ClientID:   clientID,
		CreatedAt:  time.Now(),
		LastUsedAt: time.Now(),
		Context:    txCtx,
		Cancel:     cancel,
		Handle:     handle,
	}
	
	m.transactions[txID] = tx
	m.byClient[clientID] = append(clientTxs, txID)
	
	return tx, nil
}

// Get retrieves a transaction by ID
func (m *Manager) Get(txID string) (*Transaction, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	tx, ok := m.transactions[txID]
	if !ok {
		return nil, fmt.Errorf("transaction not found: %s", txID)
	}
	
	return tx, nil
}

// Touch updates the last used time for a transaction
func (m *Manager) Touch(txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	tx, ok := m.transactions[txID]
	if !ok {
		return fmt.Errorf("transaction not found: %s", txID)
	}
	
	tx.LastUsedAt = time.Now()
	return nil
}

// Remove removes a transaction
func (m *Manager) Remove(txID string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	tx, ok := m.transactions[txID]
	if !ok {
		return nil // Already removed
	}
	
	// Cancel context
	tx.Cancel()
	
	// Remove from maps
	delete(m.transactions, txID)
	
	// Remove from client list
	clientTxs := m.byClient[tx.ClientID]
	for i, id := range clientTxs {
		if id == txID {
			m.byClient[tx.ClientID] = append(clientTxs[:i], clientTxs[i+1:]...)
			break
		}
	}
	
	// Clean up empty client entry
	if len(m.byClient[tx.ClientID]) == 0 {
		delete(m.byClient, tx.ClientID)
	}
	
	return nil
}

// Stats returns transaction statistics
func (m *Manager) Stats() (total, byClientCount int) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	
	return len(m.transactions), len(m.byClient)
}

// Close stops the manager and cancels all transactions
func (m *Manager) Close() error {
	close(m.stopCh)
	m.wg.Wait()
	
	m.mu.Lock()
	defer m.mu.Unlock()
	
	// Cancel all transactions
	for _, tx := range m.transactions {
		tx.Cancel()
	}
	
	m.transactions = make(map[string]*Transaction)
	m.byClient = make(map[string][]string)
	
	return nil
}

// cleanupLoop periodically removes idle transactions
func (m *Manager) cleanupLoop() {
	defer m.wg.Done()
	
	ticker := time.NewTicker(m.idleTimeout / 2)
	defer ticker.Stop()
	
	for {
		select {
		case <-m.stopCh:
			return
		case <-ticker.C:
			m.cleanupIdle()
		}
	}
}

// cleanupIdle removes transactions that have been idle too long
func (m *Manager) cleanupIdle() {
	m.mu.Lock()
	defer m.mu.Unlock()
	
	now := time.Now()
	var toRemove []string
	
	for txID, tx := range m.transactions {
		if now.Sub(tx.LastUsedAt) > m.idleTimeout {
			toRemove = append(toRemove, txID)
		}
	}
	
	// Remove idle transactions
	for _, txID := range toRemove {
		tx := m.transactions[txID]
		tx.Cancel()
		delete(m.transactions, txID)
		
		// Remove from client list
		clientTxs := m.byClient[tx.ClientID]
		for i, id := range clientTxs {
			if id == txID {
				m.byClient[tx.ClientID] = append(clientTxs[:i], clientTxs[i+1:]...)
				break
			}
		}
		
		if len(m.byClient[tx.ClientID]) == 0 {
			delete(m.byClient, tx.ClientID)
		}
	}
}

// generateTxID generates a random transaction ID
func generateTxID() (string, error) {
	b := make([]byte, 16)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	return hex.EncodeToString(b), nil
}
