package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/api"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/db"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/semaphore"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/txmanager"
)

// setupBenchmarkServer creates a test server for benchmarking
func setupBenchmarkServer(b *testing.B) (*httptest.Server, func()) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "mindb-bench-*")
	if err != nil {
		b.Fatal(err)
	}

	// Create database adapter
	adapter, err := db.NewAdapter(tmpDir)
	if err != nil {
		os.RemoveAll(tmpDir)
		b.Fatal(err)
	}

	// Create transaction manager
	txMgr := txmanager.NewManager(60*time.Second, 100, 5)

	// Create execution semaphore
	execSem := semaphore.New(32)

	// Create logger
	logger := zerolog.Nop()

	// Create handlers
	handlers := api.NewHandlers(adapter, txMgr, execSem, logger, 5*time.Second)

	// Setup router
	r := chi.NewRouter()
	r.Get("/health", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"healthy"}`))
	})
	r.Post("/query", handlers.QueryHandler())
	r.Post("/execute", handlers.ExecuteHandler())
	r.Post("/tx/begin", handlers.TxBeginHandler())
	r.Post("/tx/{txID}/exec", handlers.TxExecHandler())
	r.Post("/tx/{txID}/commit", handlers.TxCommitHandler())
	r.Post("/tx/{txID}/rollback", handlers.TxRollbackHandler())

	// Create test server
	server := httptest.NewServer(r)

	cleanup := func() {
		server.Close()
		os.RemoveAll(tmpDir)
	}

	return server, cleanup
}

// BenchmarkHealthEndpoint benchmarks the health check endpoint
func BenchmarkHealthEndpoint(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		resp, err := http.Get(server.URL + "/health")
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

// BenchmarkCreateTable benchmarks table creation
func BenchmarkCreateTable(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	// Create database first
	createDB := map[string]string{"sql": "CREATE DATABASE benchdb"}
	body, _ := json.Marshal(createDB)
	resp, _ := http.Post(server.URL+"/execute", "application/json", bytes.NewBuffer(body))
	resp.Body.Close()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tableName := fmt.Sprintf("table_%d", i)
		req := map[string]string{
			"sql": fmt.Sprintf("CREATE TABLE %s(id INT PRIMARY KEY, name TEXT, value INT)", tableName),
		}
		body, _ := json.Marshal(req)
		
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

// BenchmarkInsertSingle benchmarks single row inserts
func BenchmarkInsertSingle(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	// Setup
	setupSQL := []string{
		"CREATE DATABASE benchdb",
		"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)",
	}
	
	for _, sql := range setupSQL {
		req := map[string]string{"sql": sql}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		if sql != setupSQL[0] {
			httpReq.Header.Set("X-Mindb-Database", "benchdb")
		}
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := map[string]string{
			"sql": fmt.Sprintf("INSERT INTO users(id,name,age) VALUES(%d,\"User%d\",%d)", i+10000, i, 20+i%50),
		}
		body, _ := json.Marshal(req)
		
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

// BenchmarkSelectAll benchmarks SELECT * queries
func BenchmarkSelectAll(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	// Setup with data
	setupSQL := []string{
		"CREATE DATABASE benchdb",
		"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)",
	}
	
	for _, sql := range setupSQL {
		req := map[string]string{"sql": sql}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		if sql != setupSQL[0] {
			httpReq.Header.Set("X-Mindb-Database", "benchdb")
		}
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		req := map[string]string{
			"sql": fmt.Sprintf("INSERT INTO users(id,name,age) VALUES(%d,\"User%d\",%d)", i, i, 20+i%50),
		}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := map[string]string{"sql": "SELECT * FROM users"}
		body, _ := json.Marshal(req)
		
		httpReq, _ := http.NewRequest("POST", server.URL+"/query", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

// BenchmarkSelectWithWhere benchmarks SELECT with WHERE clause
func BenchmarkSelectWithWhere(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	// Setup with data
	setupSQL := []string{
		"CREATE DATABASE benchdb",
		"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)",
	}
	
	for _, sql := range setupSQL {
		req := map[string]string{"sql": sql}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		if sql != setupSQL[0] {
			httpReq.Header.Set("X-Mindb-Database", "benchdb")
		}
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	// Insert test data
	for i := 0; i < 1000; i++ {
		req := map[string]string{
			"sql": fmt.Sprintf("INSERT INTO users(id,name,age) VALUES(%d,\"User%d\",%d)", i, i, 20+i%50),
		}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := map[string]string{"sql": "SELECT * FROM users WHERE age > 30"}
		body, _ := json.Marshal(req)
		
		httpReq, _ := http.NewRequest("POST", server.URL+"/query", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

// BenchmarkUpdate benchmarks UPDATE operations
func BenchmarkUpdate(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	// Setup with data
	setupSQL := []string{
		"CREATE DATABASE benchdb",
		"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)",
	}
	
	for _, sql := range setupSQL {
		req := map[string]string{"sql": sql}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		if sql != setupSQL[0] {
			httpReq.Header.Set("X-Mindb-Database", "benchdb")
		}
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		req := map[string]string{
			"sql": fmt.Sprintf("INSERT INTO users(id,name,age) VALUES(%d,\"User%d\",%d)", i, i, 20+i%50),
		}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		id := i % 100
		req := map[string]string{
			"sql": fmt.Sprintf("UPDATE users SET age=%d WHERE id=%d", 25+i%40, id),
		}
		body, _ := json.Marshal(req)
		
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}

// BenchmarkTransaction benchmarks transaction operations
func BenchmarkTransaction(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	// Setup
	setupSQL := []string{
		"CREATE DATABASE benchdb",
		"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)",
	}
	
	for _, sql := range setupSQL {
		req := map[string]string{"sql": sql}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		if sql != setupSQL[0] {
			httpReq.Header.Set("X-Mindb-Database", "benchdb")
		}
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	// Insert initial data
	for i := 0; i < 10; i++ {
		req := map[string]string{
			"sql": fmt.Sprintf("INSERT INTO users(id,name,age) VALUES(%d,\"User%d\",%d)", i, i, 20+i%50),
		}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Begin transaction
		resp, _ := http.Post(server.URL+"/tx/begin", "application/json", nil)
		var txResp map[string]string
		json.NewDecoder(resp.Body).Decode(&txResp)
		txID := txResp["tx"]
		resp.Body.Close()

		// Execute in transaction
		id := i % 10
		req := map[string]string{
			"sql": fmt.Sprintf("UPDATE users SET age=%d WHERE id=%d", 25+i%40, id),
		}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/tx/"+txID+"/exec", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		resp, _ = http.DefaultClient.Do(httpReq)
		resp.Body.Close()

		// Commit
		resp, _ = http.Post(server.URL+"/tx/"+txID+"/commit", "application/json", nil)
		resp.Body.Close()
	}
}

// BenchmarkConcurrentReads benchmarks concurrent read operations
func BenchmarkConcurrentReads(b *testing.B) {
	server, cleanup := setupBenchmarkServer(b)
	defer cleanup()

	// Setup with data
	setupSQL := []string{
		"CREATE DATABASE benchdb",
		"CREATE TABLE users(id INT PRIMARY KEY, name TEXT, age INT)",
	}
	
	for _, sql := range setupSQL {
		req := map[string]string{"sql": sql}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		if sql != setupSQL[0] {
			httpReq.Header.Set("X-Mindb-Database", "benchdb")
		}
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	// Insert test data
	for i := 0; i < 100; i++ {
		req := map[string]string{
			"sql": fmt.Sprintf("INSERT INTO users(id,name,age) VALUES(%d,\"User%d\",%d)", i, i, 20+i%50),
		}
		body, _ := json.Marshal(req)
		httpReq, _ := http.NewRequest("POST", server.URL+"/execute", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		resp, _ := http.DefaultClient.Do(httpReq)
		resp.Body.Close()
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		req := map[string]string{"sql": "SELECT * FROM users WHERE age > 25"}
		body, _ := json.Marshal(req)
		
		httpReq, _ := http.NewRequest("POST", server.URL+"/query", bytes.NewBuffer(body))
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("X-Mindb-Database", "benchdb")
		
		resp, err := http.DefaultClient.Do(httpReq)
		if err != nil {
			b.Fatal(err)
		}
		resp.Body.Close()
	}
}
