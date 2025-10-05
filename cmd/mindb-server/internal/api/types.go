package api

import "time"

// QueryRequest represents a read-only query request
type QueryRequest struct {
	SQL       string        `json:"sql"`
	Args      []interface{} `json:"args,omitempty"`
	Limit     int           `json:"limit,omitempty"`
	TimeoutMS int           `json:"timeout_ms,omitempty"`
}

// QueryResponse represents a query response
type QueryResponse struct {
	Columns   []string        `json:"columns"`
	Rows      [][]interface{} `json:"rows"`
	RowCount  int             `json:"row_count"`
	Truncated bool            `json:"truncated"`
	LatencyMS int64           `json:"latency_ms"`
}

// ExecuteRequest represents a DML/DDL execution request
type ExecuteRequest struct {
	SQL       string        `json:"sql"`
	Args      []interface{} `json:"args,omitempty"`
	TimeoutMS int           `json:"timeout_ms,omitempty"`
}

// ExecuteResponse represents an execution response
type ExecuteResponse struct {
	AffectedRows int             `json:"affected_rows"`
	Returning    *QueryResponse  `json:"returning,omitempty"`
	LatencyMS    int64           `json:"latency_ms"`
}

// TxBeginResponse represents a transaction begin response
type TxBeginResponse struct {
	TxID      string `json:"tx"`
	Isolation string `json:"isolation"`
}

// TxStatusResponse represents a transaction status response
type TxStatusResponse struct {
	Status string `json:"status"`
}

// ErrorResponse represents an error response
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// ErrorDetail contains error details
type ErrorDetail struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Error codes
const (
	ErrCodeBadRequest     = "BAD_REQUEST"
	ErrCodeTimeout        = "TIMEOUT"
	ErrCodeConflict       = "CONFLICT"
	ErrCodeNotFound       = "NOT_FOUND"
	ErrCodeInternal       = "INTERNAL_ERROR"
	ErrCodeUnauthorized   = "UNAUTHORIZED"
	ErrCodeTooManyTx      = "TOO_MANY_TRANSACTIONS"
	ErrCodeInvalidSQL     = "INVALID_SQL"
)

// Timeout returns the timeout duration or default
func (r *QueryRequest) Timeout(defaultTimeout time.Duration) time.Duration {
	if r.TimeoutMS > 0 {
		return time.Duration(r.TimeoutMS) * time.Millisecond
	}
	return defaultTimeout
}

// Timeout returns the timeout duration or default
func (r *ExecuteRequest) Timeout(defaultTimeout time.Duration) time.Duration {
	if r.TimeoutMS > 0 {
		return time.Duration(r.TimeoutMS) * time.Millisecond
	}
	return defaultTimeout
}

// BatchQueryRequest represents a batch query request
type BatchQueryRequest struct {
	Queries []string `json:"queries"`
}

// BatchQueryResult represents a single query result in a batch
type BatchQueryResult struct {
	Result string `json:"result,omitempty"`
	Error  string `json:"error,omitempty"`
}

// BatchQueryResponse represents a batch query response
type BatchQueryResponse struct {
	Results   []BatchQueryResult `json:"results"`
	TotalTime int64              `json:"total_time_ms"`
}
