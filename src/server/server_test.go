package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/sausheong/mindb/src/server/internal/api"
	"github.com/sausheong/mindb/src/server/internal/db"
	"github.com/sausheong/mindb/src/server/internal/semaphore"
	"github.com/sausheong/mindb/src/server/internal/txmanager"
)

func setupTestServer(t *testing.T) (*chi.Mux, *db.Adapter, func()) {
	tmpDir := t.TempDir()
	
	// Create database
	database, err := db.NewAdapter(tmpDir)
	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	// Create transaction manager
	txMgr := txmanager.NewManager(1*time.Minute, 100, 5)

	// Create semaphore
	execSem := semaphore.New(32)

	// Create logger
	logger := zerolog.New(os.Stdout).Level(zerolog.ErrorLevel)

	// Create handlers
	handlers := api.NewHandlers(database, txMgr, execSem, logger, 2*time.Second)

	// Setup router
	r := chi.NewRouter()
	r.Post("/query", handlers.QueryHandler())
	r.Post("/execute", handlers.ExecuteHandler())
	r.Post("/tx/begin", handlers.TxBeginHandler())
	r.Post("/tx/{txID}/exec", handlers.TxExecHandler())
	r.Post("/tx/{txID}/commit", handlers.TxCommitHandler())
	r.Post("/tx/{txID}/rollback", handlers.TxRollbackHandler())
	r.Get("/health", handlers.HealthHandler())
	r.Get("/stream", handlers.StreamHandler())

	cleanup := func() {
		txMgr.Close()
		database.Close()
	}

	return r, database, cleanup
}

func TestHealthEndpoint(t *testing.T) {
	router, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("GET", "/health", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp map[string]interface{}
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp["status"] != "healthy" {
		t.Errorf("Expected status 'healthy', got %v", resp["status"])
	}
}

func TestQueryEndpoint_InvalidJSON(t *testing.T) {
	router, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("POST", "/query", bytes.NewBufferString("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestQueryEndpoint_MissingSQL(t *testing.T) {
	router, _, cleanup := setupTestServer(t)
	defer cleanup()

	reqBody := api.QueryRequest{
		SQL: "",
	}
	body, _ := json.Marshal(reqBody)

	req := httptest.NewRequest("POST", "/query", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}

	var errResp api.ErrorResponse
	json.NewDecoder(w.Body).Decode(&errResp)

	if errResp.Error.Code != api.ErrCodeBadRequest {
		t.Errorf("Expected error code %s, got %s", api.ErrCodeBadRequest, errResp.Error.Code)
	}
}

func TestExecuteEndpoint_InvalidJSON(t *testing.T) {
	router, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("POST", "/execute", bytes.NewBufferString("invalid json"))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("Expected status 400, got %d", w.Code)
	}
}

func TestTxBeginEndpoint(t *testing.T) {
	router, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("POST", "/tx/begin", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("Expected status 200, got %d", w.Code)
	}

	var resp api.TxBeginResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("Failed to decode response: %v", err)
	}

	if resp.TxID == "" {
		t.Error("Expected non-empty transaction ID")
	}

	if resp.Isolation != "snapshot" {
		t.Errorf("Expected isolation 'snapshot', got %s", resp.Isolation)
	}
}

func TestTxCommit_NotFound(t *testing.T) {
	router, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("POST", "/tx/nonexistent/commit", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestTxRollback_NotFound(t *testing.T) {
	router, _, cleanup := setupTestServer(t)
	defer cleanup()

	req := httptest.NewRequest("POST", "/tx/nonexistent/rollback", nil)
	w := httptest.NewRecorder()

	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected status 404, got %d", w.Code)
	}
}

func TestFullWorkflow(t *testing.T) {
	t.Skip("Skipping full workflow test - requires mindb to be refactored as importable library")
	
	router, database, cleanup := setupTestServer(t)
	defer cleanup()

	// Setup: Create database and table
	// TODO: Re-enable when mindb is refactored
	_ = database
	_ = router

	// Create table
	createReq := api.ExecuteRequest{
		SQL: "CREATE TABLE users(id INT PRIMARY KEY, name TEXT)",
	}
	body, _ := json.Marshal(createReq)
	req := httptest.NewRequest("POST", "/execute", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Create table failed: %d - %s", w.Code, w.Body.String())
	}

	// Insert data
	insertReq := api.ExecuteRequest{
		SQL:  "INSERT INTO users(id, name) VALUES($1, $2)",
		Args: []interface{}{1, "Alice"},
	}
	body, _ = json.Marshal(insertReq)
	req = httptest.NewRequest("POST", "/execute", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Insert failed: %d - %s", w.Code, w.Body.String())
	}

	var execResp api.ExecuteResponse
	json.NewDecoder(w.Body).Decode(&execResp)
	if execResp.AffectedRows != 1 {
		t.Errorf("Expected 1 affected row, got %d", execResp.AffectedRows)
	}

	// Query data
	queryReq := api.QueryRequest{
		SQL:  "SELECT id, name FROM users WHERE id = $1",
		Args: []interface{}{1},
	}
	body, _ = json.Marshal(queryReq)
	req = httptest.NewRequest("POST", "/query", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Query failed: %d - %s", w.Code, w.Body.String())
	}

	var queryResp api.QueryResponse
	json.NewDecoder(w.Body).Decode(&queryResp)
	if queryResp.RowCount != 1 {
		t.Errorf("Expected 1 row, got %d", queryResp.RowCount)
	}

	// Update
	updateReq := api.ExecuteRequest{
		SQL:  "UPDATE users SET name = $1 WHERE id = $2",
		Args: []interface{}{"Bob", 1},
	}
	body, _ = json.Marshal(updateReq)
	req = httptest.NewRequest("POST", "/execute", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Update failed: %d - %s", w.Code, w.Body.String())
	}

	// Delete
	deleteReq := api.ExecuteRequest{
		SQL:  "DELETE FROM users WHERE id = $1",
		Args: []interface{}{1},
	}
	body, _ = json.Marshal(deleteReq)
	req = httptest.NewRequest("POST", "/execute", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Delete failed: %d - %s", w.Code, w.Body.String())
	}
}

func TestTransactionWorkflow(t *testing.T) {
	t.Skip("Skipping transaction workflow test - requires mindb to be refactored as importable library")
	
	router, database, cleanup := setupTestServer(t)
	defer cleanup()

	// Setup
	// TODO: Re-enable when mindb is refactored
	_ = database
	_ = router

	// Create table
	createReq := api.ExecuteRequest{
		SQL: "CREATE TABLE accounts(id INT PRIMARY KEY, balance INT)",
	}
	body, _ := json.Marshal(createReq)
	req := httptest.NewRequest("POST", "/execute", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	// Begin transaction
	req = httptest.NewRequest("POST", "/tx/begin", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Begin failed: %d", w.Code)
	}

	var beginResp api.TxBeginResponse
	json.NewDecoder(w.Body).Decode(&beginResp)
	txID := beginResp.TxID

	// Execute in transaction
	execReq := api.ExecuteRequest{
		SQL:  "INSERT INTO accounts(id, balance) VALUES($1, $2)",
		Args: []interface{}{1, 100},
	}
	body, _ = json.Marshal(execReq)
	req = httptest.NewRequest("POST", "/tx/"+txID+"/exec", bytes.NewBuffer(body))
	req.Header.Set("Content-Type", "application/json")
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Exec failed: %d - %s", w.Code, w.Body.String())
	}

	// Commit transaction
	req = httptest.NewRequest("POST", "/tx/"+txID+"/commit", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Commit failed: %d", w.Code)
	}

	var commitResp api.TxStatusResponse
	json.NewDecoder(w.Body).Decode(&commitResp)
	if commitResp.Status != "committed" {
		t.Errorf("Expected status 'committed', got %s", commitResp.Status)
	}
}

func TestTransactionRollback(t *testing.T) {
	t.Skip("Skipping transaction rollback test - requires mindb to be refactored as importable library")
	
	router, database, cleanup := setupTestServer(t)
	defer cleanup()

	// Setup
	// TODO: Re-enable when mindb is refactored
	_ = database
	_ = router

	// Begin transaction
	req := httptest.NewRequest("POST", "/tx/begin", nil)
	w := httptest.NewRecorder()
	router.ServeHTTP(w, req)

	var beginResp api.TxBeginResponse
	json.NewDecoder(w.Body).Decode(&beginResp)
	txID := beginResp.TxID

	// Rollback transaction
	req = httptest.NewRequest("POST", "/tx/"+txID+"/rollback", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("Rollback failed: %d", w.Code)
	}

	var rollbackResp api.TxStatusResponse
	json.NewDecoder(w.Body).Decode(&rollbackResp)
	if rollbackResp.Status != "rolled_back" {
		t.Errorf("Expected status 'rolled_back', got %s", rollbackResp.Status)
	}

	// Transaction should no longer exist
	req = httptest.NewRequest("POST", "/tx/"+txID+"/commit", nil)
	w = httptest.NewRecorder()
	router.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Errorf("Expected 404 for removed transaction, got %d", w.Code)
	}
}
