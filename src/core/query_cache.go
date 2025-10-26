package mindb

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// CachedResult represents a cached query result
type CachedResult struct {
	Rows      []Row
	Timestamp time.Time
	HitCount  int64
}

// QueryCache caches query results with TTL and LRU eviction
type QueryCache struct {
	cache      map[string]*CachedResult
	mu         sync.RWMutex
	ttl        time.Duration
	maxEntries int
	hits       int64
	misses     int64
}

// NewQueryCache creates a new query cache
func NewQueryCache(ttl time.Duration, maxEntries int) *QueryCache {
	qc := &QueryCache{
		cache:      make(map[string]*CachedResult),
		ttl:        ttl,
		maxEntries: maxEntries,
	}
	
	// Start cleanup goroutine
	go qc.cleanupExpired()
	
	return qc
}

// Get retrieves a cached result
func (qc *QueryCache) Get(table string, conditions []Condition) ([]Row, bool) {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	
	key := qc.generateKey(table, conditions)
	result, exists := qc.cache[key]
	
	if !exists {
		qc.misses++
		return nil, false
	}
	
	// Check if expired
	if time.Since(result.Timestamp) > qc.ttl {
		qc.misses++
		return nil, false
	}
	
	qc.hits++
	result.HitCount++
	return result.Rows, true
}

// Put stores a result in the cache
func (qc *QueryCache) Put(table string, conditions []Condition, rows []Row) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	// Check if we need to evict
	if len(qc.cache) >= qc.maxEntries {
		qc.evictLRU()
	}
	
	key := qc.generateKey(table, conditions)
	qc.cache[key] = &CachedResult{
		Rows:      rows,
		Timestamp: time.Now(),
		HitCount:  0,
	}
}

// Invalidate removes cached results for a table
func (qc *QueryCache) Invalidate(table string) {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	// Remove all entries for this table
	for key := range qc.cache {
		if qc.keyMatchesTable(key, table) {
			delete(qc.cache, key)
		}
	}
}

// InvalidateAll clears the entire cache
func (qc *QueryCache) InvalidateAll() {
	qc.mu.Lock()
	defer qc.mu.Unlock()
	
	qc.cache = make(map[string]*CachedResult)
}

// Stats returns cache statistics
func (qc *QueryCache) Stats() map[string]interface{} {
	qc.mu.RLock()
	defer qc.mu.RUnlock()
	
	total := qc.hits + qc.misses
	hitRate := 0.0
	if total > 0 {
		hitRate = float64(qc.hits) / float64(total) * 100
	}
	
	return map[string]interface{}{
		"entries":   len(qc.cache),
		"max_entries": qc.maxEntries,
		"hits":      qc.hits,
		"misses":    qc.misses,
		"hit_rate":  fmt.Sprintf("%.2f%%", hitRate),
		"ttl_seconds": qc.ttl.Seconds(),
	}
}

// generateKey creates a cache key from table and conditions
func (qc *QueryCache) generateKey(table string, conditions []Condition) string {
	// Create a deterministic key from table and conditions
	data := struct {
		Table      string
		Conditions []Condition
	}{
		Table:      table,
		Conditions: conditions,
	}
	
	jsonData, _ := json.Marshal(data)
	hash := sha256.Sum256(jsonData)
	return fmt.Sprintf("%s:%x", table, hash[:8])
}

// keyMatchesTable checks if a cache key belongs to a table
func (qc *QueryCache) keyMatchesTable(key, table string) bool {
	// Keys are formatted as "table:hash"
	return len(key) > len(table) && key[:len(table)] == table && key[len(table)] == ':'
}

// evictLRU evicts the least recently used entry
func (qc *QueryCache) evictLRU() {
	var oldestKey string
	var oldestTime time.Time
	
	for key, result := range qc.cache {
		if oldestKey == "" || result.Timestamp.Before(oldestTime) {
			oldestKey = key
			oldestTime = result.Timestamp
		}
	}
	
	if oldestKey != "" {
		delete(qc.cache, oldestKey)
	}
}

// cleanupExpired removes expired entries periodically
func (qc *QueryCache) cleanupExpired() {
	ticker := time.NewTicker(qc.ttl / 2)
	defer ticker.Stop()
	
	for range ticker.C {
		qc.mu.Lock()
		now := time.Now()
		for key, result := range qc.cache {
			if now.Sub(result.Timestamp) > qc.ttl {
				delete(qc.cache, key)
			}
		}
		qc.mu.Unlock()
	}
}
