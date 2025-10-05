package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/db"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/semaphore"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/txmanager"
)

// Handlers holds all HTTP handlers
type Handlers struct {
	db            *db.Adapter
	txMgr         *txmanager.Manager
	execSem       *semaphore.Semaphore
	logger        zerolog.Logger
	stmtTimeout   time.Duration
}

// NewHandlers creates a new handlers instance
func NewHandlers(
	db *db.Adapter,
	txMgr *txmanager.Manager,
	execSem *semaphore.Semaphore,
	logger zerolog.Logger,
	stmtTimeout time.Duration,
) *Handlers {
	return &Handlers{
		db:          db,
		txMgr:       txMgr,
		execSem:     execSem,
		logger:      logger,
		stmtTimeout: stmtTimeout,
	}
}

// QueryHandler handles POST /query
func (h *Handlers) QueryHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Parse request
		var req QueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid JSON: "+err.Error())
			return
		}

		// Validate
		if req.SQL == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "sql is required")
			return
		}

		// Acquire execution slot
		ctx := r.Context()
		if err := h.execSem.Acquire(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, ErrCodeInternal, "server busy")
			return
		}
		defer h.execSem.Release()

		// Create timeout context
		timeout := req.Timeout(h.stmtTimeout)
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Get database from header (X-Mindb-Database)
		database := r.Header.Get("X-Mindb-Database")

		// Execute query
		columns, rows, err := h.db.Query(ctx, req.SQL, req.Args, req.Limit, database)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				writeError(w, http.StatusRequestTimeout, ErrCodeTimeout, "query timeout")
				return
			}
			writeError(w, http.StatusBadRequest, ErrCodeInvalidSQL, err.Error())
			return
		}

		// Build response
		truncated := req.Limit > 0 && len(rows) >= req.Limit
		resp := QueryResponse{
			Columns:   columns,
			Rows:      rows,
			RowCount:  len(rows),
			Truncated: truncated,
			LatencyMS: time.Since(start).Milliseconds(),
		}

		// Log
		h.logger.Info().
			Str("sql", req.SQL).
			Int("rows", len(rows)).
			Int64("latency_ms", resp.LatencyMS).
			Bool("truncated", truncated).
			Msg("query_executed")

		writeJSON(w, http.StatusOK, resp)
	}
}

// ExecuteHandler handles POST /execute
func (h *Handlers) ExecuteHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Parse request
		var req ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid JSON: "+err.Error())
			return
		}

		// Validate
		if req.SQL == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "sql is required")
			return
		}

		// Acquire execution slot
		ctx := r.Context()
		if err := h.execSem.Acquire(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, ErrCodeInternal, "server busy")
			return
		}
		defer h.execSem.Release()

		// Create timeout context
		timeout := req.Timeout(h.stmtTimeout)
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Get database from header (X-Mindb-Database)
		database := r.Header.Get("X-Mindb-Database")

		// Execute statement
		affectedRows, returning, err := h.db.Execute(ctx, req.SQL, req.Args, database)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				writeError(w, http.StatusRequestTimeout, ErrCodeTimeout, "execution timeout")
				return
			}
			writeError(w, http.StatusBadRequest, ErrCodeInvalidSQL, err.Error())
			return
		}

		// Build response
		resp := ExecuteResponse{
			AffectedRows: affectedRows,
			LatencyMS:    time.Since(start).Milliseconds(),
		}

		if returning != nil {
			resp.Returning = &QueryResponse{
				Columns:  returning.Columns,
				Rows:     returning.Rows,
				RowCount: len(returning.Rows),
			}
		}

		// Log
		h.logger.Info().
			Str("sql", req.SQL).
			Int("affected_rows", affectedRows).
			Int64("latency_ms", resp.LatencyMS).
			Msg("execute_completed")

		writeJSON(w, http.StatusOK, resp)
	}
}

// TxBeginHandler handles POST /tx/begin
func (h *Handlers) TxBeginHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		clientID := getClientID(r)

		// Begin transaction (placeholder - actual tx creation would use mindb API)
		tx, err := h.txMgr.Begin(r.Context(), clientID, nil)
		if err != nil {
			writeError(w, http.StatusConflict, ErrCodeTooManyTx, err.Error())
			return
		}

		// Log
		h.logger.Info().
			Str("tx_id", tx.ID).
			Str("client_id", clientID).
			Msg("transaction_begin")

		// Response
		resp := TxBeginResponse{
			TxID:      tx.ID,
			Isolation: "snapshot",
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// TxExecHandler handles POST /tx/{txID}/exec
func (h *Handlers) TxExecHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		txID := chi.URLParam(r, "txID")

		// Get transaction (verify it exists)
		_, err := h.txMgr.Get(txID)
		if err != nil {
			writeError(w, http.StatusNotFound, ErrCodeNotFound, "transaction not found")
			return
		}

		// Touch transaction to extend idle timeout
		h.txMgr.Touch(txID)

		// Parse request
		var req ExecuteRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid JSON: "+err.Error())
			return
		}

		// Validate
		if req.SQL == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "sql is required")
			return
		}

		// Use request context instead of tx.Context to avoid cancellation issues
		ctx := r.Context()

		// Acquire execution slot
		if err := h.execSem.Acquire(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, ErrCodeInternal, "server busy")
			return
		}
		defer h.execSem.Release()

		// Create timeout context
		timeout := req.Timeout(h.stmtTimeout)
		ctx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()

		// Get database from header (X-Mindb-Database)
		database := r.Header.Get("X-Mindb-Database")

		// Execute within transaction
		affectedRows, returning, err := h.db.Execute(ctx, req.SQL, req.Args, database)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				writeError(w, http.StatusRequestTimeout, ErrCodeTimeout, "execution timeout")
				return
			}
			writeError(w, http.StatusBadRequest, ErrCodeInvalidSQL, err.Error())
			return
		}

		// Build response
		resp := ExecuteResponse{
			AffectedRows: affectedRows,
			LatencyMS:    time.Since(start).Milliseconds(),
		}

		if returning != nil {
			resp.Returning = &QueryResponse{
				Columns:  returning.Columns,
				Rows:     returning.Rows,
				RowCount: len(returning.Rows),
			}
		}

		// Log
		h.logger.Info().
			Str("tx_id", txID).
			Str("sql", req.SQL).
			Int("affected_rows", affectedRows).
			Int64("latency_ms", resp.LatencyMS).
			Msg("tx_execute_completed")

		writeJSON(w, http.StatusOK, resp)
	}
}

// TxCommitHandler handles POST /tx/{txID}/commit
func (h *Handlers) TxCommitHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		txID := chi.URLParam(r, "txID")

		// Get transaction
		_, err := h.txMgr.Get(txID)
		if err != nil {
			writeError(w, http.StatusNotFound, ErrCodeNotFound, "transaction not found")
			return
		}

		// Commit transaction (placeholder - actual commit would use mindb API)
		// tx.Handle.(*mindb.Tx).Commit()

		// Remove from manager
		h.txMgr.Remove(txID)

		// Log
		h.logger.Info().
			Str("tx_id", txID).
			Msg("transaction_committed")

		// Response
		resp := TxStatusResponse{
			Status: "committed",
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// TxRollbackHandler handles POST /tx/{txID}/rollback
func (h *Handlers) TxRollbackHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		txID := chi.URLParam(r, "txID")

		// Get transaction
		_, err := h.txMgr.Get(txID)
		if err != nil {
			writeError(w, http.StatusNotFound, ErrCodeNotFound, "transaction not found")
			return
		}

		// Rollback transaction (placeholder - actual rollback would use mindb API)
		// tx.Handle.(*mindb.Tx).Rollback()

		// Remove from manager
		h.txMgr.Remove(txID)

		// Log
		h.logger.Info().
			Str("tx_id", txID).
			Msg("transaction_rolled_back")

		// Response
		resp := TxStatusResponse{
			Status: "rolled_back",
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// HealthHandler handles GET /health
func (h *Handlers) HealthHandler() http.HandlerFunc {
	startTime := time.Now()

	return func(w http.ResponseWriter, r *http.Request) {
		openTx, _ := h.txMgr.Stats()

		resp := map[string]interface{}{
			"status":             "healthy",
			"uptime_seconds":     int(time.Since(startTime).Seconds()),
			"open_transactions":  openTx,
			"available_exec_slots": h.execSem.Available(),
		}

		writeJSON(w, http.StatusOK, resp)
	}
}

// Helper functions

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	json.NewEncoder(w).Encode(data)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	resp := ErrorResponse{
		Error: ErrorDetail{
			Code:    code,
			Message: message,
		},
	}
	writeJSON(w, status, resp)
}

func getClientID(r *http.Request) string {
	// Use remote address as client ID
	// In production, you might use a session ID or authenticated user ID
	return r.RemoteAddr
}

// BatchQueryHandler handles POST /query/batch for batched queries
func (h *Handlers) BatchQueryHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Parse batch request
		var req BatchQueryRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid JSON: "+err.Error())
			return
		}

		if len(req.Queries) == 0 {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "no queries provided")
			return
		}

		// Get database from header
		dbName := r.Header.Get("X-Mindb-Database")

		// Execute each query
		results := make([]BatchQueryResult, len(req.Queries))
		for i, sql := range req.Queries {
			// Create context with timeout
			ctx, cancel := context.WithTimeout(r.Context(), h.stmtTimeout)
			
			// Execute query
			_, _, err := h.db.Query(ctx, sql, nil, 0, dbName)
			cancel()

			if err != nil {
				results[i] = BatchQueryResult{
					Error: err.Error(),
				}
			} else {
				results[i] = BatchQueryResult{
					Result: "success",
				}
			}
		}

		// Write response
		resp := BatchQueryResponse{
			Results:   results,
			TotalTime: time.Since(start).Milliseconds(),
		}
		writeJSON(w, http.StatusOK, resp)

		h.logger.Info().
			Int("query_count", len(req.Queries)).
			Int64("latency_ms", time.Since(start).Milliseconds()).
			Msg("batch_query_completed")
	}
}
