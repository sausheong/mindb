package api

import (
	"context"
	"encoding/json"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/rs/zerolog"
	"github.com/sausheong/mindb/src/server/internal/db"
	"github.com/sausheong/mindb/src/server/internal/semaphore"
	"github.com/sausheong/mindb/src/server/internal/txmanager"
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

// CreateProcedureHandler handles POST /procedures
func (h *Handlers) CreateProcedureHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Parse request
		var req CreateProcedureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid JSON: "+err.Error())
			return
		}

		// Validate request
		if req.Name == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "procedure name is required")
			return
		}
		if req.WasmBase64 == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "wasm_base64 is required")
			return
		}
		if req.Language == "" {
			req.Language = "wasm" // Default to wasm
		}

		// Get database from header
		dbName := r.Header.Get("X-Mindb-Database")

		// Create execution context with timeout
		ctx, cancel := context.WithTimeout(r.Context(), h.stmtTimeout)
		defer cancel()

		// Convert Params to []interface{}
		params := make([]interface{}, len(req.Params))
		for i, p := range req.Params {
			params[i] = map[string]interface{}{
				"name":      p.Name,
				"data_type": p.DataType,
			}
		}

		// Create procedure via database adapter
		err := h.db.CreateProcedure(ctx, dbName, req.Name, req.Language, req.WasmBase64, params, req.ReturnType, req.Description)
		if err != nil {
			h.logger.Error().Err(err).Str("procedure", req.Name).Msg("create_procedure_failed")
			writeError(w, http.StatusInternalServerError, ErrCodeInternal, "failed to create procedure: "+err.Error())
			return
		}

		// Write response
		resp := CreateProcedureResponse{
			Name:      req.Name,
			Message:   "Procedure created successfully",
			LatencyMS: time.Since(start).Milliseconds(),
		}
		writeJSON(w, http.StatusCreated, resp)

		h.logger.Info().
			Str("procedure", req.Name).
			Str("language", req.Language).
			Int64("latency_ms", time.Since(start).Milliseconds()).
			Msg("procedure_created")
	}
}

// DropProcedureHandler handles DELETE /procedures/:name
func (h *Handlers) DropProcedureHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Get procedure name from URL
		procName := chi.URLParam(r, "name")
		if procName == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "procedure name is required")
			return
		}

		// Get database from header
		dbName := r.Header.Get("X-Mindb-Database")

		// Create execution context with timeout
		ctx, cancel := context.WithTimeout(r.Context(), h.stmtTimeout)
		defer cancel()

		// Drop procedure via database adapter
		err := h.db.DropProcedure(ctx, dbName, procName)
		if err != nil {
			h.logger.Error().Err(err).Str("procedure", procName).Msg("drop_procedure_failed")
			writeError(w, http.StatusInternalServerError, ErrCodeInternal, "failed to drop procedure: "+err.Error())
			return
		}

		// Write response
		resp := DropProcedureResponse{
			Name:      procName,
			Message:   "Procedure dropped successfully",
			LatencyMS: time.Since(start).Milliseconds(),
		}
		writeJSON(w, http.StatusOK, resp)

		h.logger.Info().
			Str("procedure", procName).
			Int64("latency_ms", time.Since(start).Milliseconds()).
			Msg("procedure_dropped")
	}
}

// ListProceduresHandler handles GET /procedures
func (h *Handlers) ListProceduresHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Get database from header
		dbName := r.Header.Get("X-Mindb-Database")

		// Create execution context with timeout
		ctx, cancel := context.WithTimeout(r.Context(), h.stmtTimeout)
		defer cancel()

		// List procedures via database adapter
		procedures, err := h.db.ListProcedures(ctx, dbName)
		if err != nil {
			h.logger.Error().Err(err).Msg("list_procedures_failed")
			writeError(w, http.StatusInternalServerError, ErrCodeInternal, "failed to list procedures: "+err.Error())
			return
		}

		// Convert procedures to ProcedureInfo
		procInfos := make([]ProcedureInfo, len(procedures))
		for i, p := range procedures {
			if procMap, ok := p.(map[string]interface{}); ok {
				// Extract params if present
				var params []Param
				if paramsInterface, ok := procMap["params"]; ok {
					if paramsList, ok := paramsInterface.([]interface{}); ok {
						params = make([]Param, len(paramsList))
						for j, paramInterface := range paramsList {
							if paramMap, ok := paramInterface.(map[string]interface{}); ok {
								params[j] = Param{
									Name:     paramMap["name"].(string),
									DataType: paramMap["data_type"].(string),
								}
							}
						}
					}
				}
				
				procInfos[i] = ProcedureInfo{
					Name:        procMap["name"].(string),
					Language:    procMap["language"].(string),
					Params:      params,
					ReturnType:  procMap["return_type"].(string),
					Description: procMap["description"].(string),
					CreatedAt:   procMap["created_at"].(string),
					UpdatedAt:   procMap["updated_at"].(string),
				}
			}
		}

		// Write response
		resp := ListProceduresResponse{
			Procedures: procInfos,
			Count:      len(procInfos),
			LatencyMS:  time.Since(start).Milliseconds(),
		}
		writeJSON(w, http.StatusOK, resp)

		h.logger.Info().
			Int("count", len(procedures)).
			Int64("latency_ms", time.Since(start).Milliseconds()).
			Msg("procedures_listed")
	}
}

// CallProcedureHandler handles POST /procedures/:name/call
func (h *Handlers) CallProcedureHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()

		// Get procedure name from URL
		procName := chi.URLParam(r, "name")
		if procName == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "procedure name is required")
			return
		}

		// Parse request
		var req CallProcedureRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid JSON: "+err.Error())
			return
		}

		// Get database from header
		dbName := r.Header.Get("X-Mindb-Database")

		// Create execution context with timeout
		ctx, cancel := context.WithTimeout(r.Context(), h.stmtTimeout)
		defer cancel()

		// Call procedure via database adapter
		result, err := h.db.CallProcedure(ctx, dbName, procName, req.Args...)
		if err != nil {
			h.logger.Error().Err(err).Str("procedure", procName).Msg("call_procedure_failed")
			writeError(w, http.StatusInternalServerError, ErrCodeInternal, "failed to call procedure: "+err.Error())
			return
		}

		// Write response
		resp := CallProcedureResponse{
			Result:    result,
			LatencyMS: time.Since(start).Milliseconds(),
		}
		writeJSON(w, http.StatusOK, resp)

		h.logger.Info().
			Str("procedure", procName).
			Int64("latency_ms", time.Since(start).Milliseconds()).
			Msg("procedure_called")
	}
}
