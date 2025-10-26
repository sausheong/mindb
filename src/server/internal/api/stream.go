package api

import (
	"context"
	"encoding/json"
	"net/http"
	"strconv"
)

// StreamHandler handles GET /stream for NDJSON streaming
func (h *Handlers) StreamHandler() http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Parse query parameters
		sql := r.URL.Query().Get("sql")
		if sql == "" {
			writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "sql parameter is required")
			return
		}

		// Parse limit
		limit := 0
		if limitStr := r.URL.Query().Get("limit"); limitStr != "" {
			var err error
			limit, err = strconv.Atoi(limitStr)
			if err != nil {
				writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid limit parameter")
				return
			}
		}

		// Parse args (JSON-encoded array)
		var args []interface{}
		if argsStr := r.URL.Query().Get("args"); argsStr != "" {
			if err := json.Unmarshal([]byte(argsStr), &args); err != nil {
				writeError(w, http.StatusBadRequest, ErrCodeBadRequest, "invalid args parameter")
				return
			}
		}

		// Acquire execution slot
		ctx := r.Context()
		if err := h.execSem.Acquire(ctx); err != nil {
			writeError(w, http.StatusServiceUnavailable, ErrCodeInternal, "server busy")
			return
		}
		defer h.execSem.Release()

		// Create timeout context
		ctx, cancel := context.WithTimeout(ctx, h.stmtTimeout)
		defer cancel()

		// Get database from header (X-Mindb-Database)
		database := r.Header.Get("X-Mindb-Database")

		// Execute query
		columns, rows, err := h.db.Query(ctx, sql, args, limit, database)
		if err != nil {
			if ctx.Err() == context.DeadlineExceeded {
				writeError(w, http.StatusRequestTimeout, ErrCodeTimeout, "query timeout")
				return
			}
			writeError(w, http.StatusBadRequest, ErrCodeInvalidSQL, err.Error())
			return
		}

		// Set headers for NDJSON streaming
		w.Header().Set("Content-Type", "application/x-ndjson")
		w.Header().Set("Transfer-Encoding", "chunked")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.WriteHeader(http.StatusOK)

		// Get flusher
		flusher, ok := w.(http.Flusher)
		if !ok {
			h.logger.Error().Msg("streaming not supported")
			return
		}

		// Stream rows as NDJSON
		encoder := json.NewEncoder(w)
		rowCount := 0
		flushInterval := 100 // Flush every 100 rows

		for _, row := range rows {
			// Check if client disconnected
			select {
			case <-ctx.Done():
				h.logger.Info().
					Int("rows_sent", rowCount).
					Msg("stream_cancelled")
				return
			default:
			}

			// Build row object
			rowObj := make(map[string]interface{})
			for i, col := range columns {
				if i < len(row) {
					rowObj[col] = row[i]
				}
			}

			// Encode and write row
			if err := encoder.Encode(rowObj); err != nil {
				h.logger.Error().Err(err).Msg("failed to encode row")
				return
			}

			rowCount++

			// Periodic flush
			if rowCount%flushInterval == 0 {
				flusher.Flush()
			}
		}

		// Final flush
		flusher.Flush()

		h.logger.Info().
			Str("sql", sql).
			Int("rows", rowCount).
			Msg("stream_completed")
	}
}
