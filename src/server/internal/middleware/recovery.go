package middleware

import (
	"fmt"
	"net/http"
	"runtime/debug"

	"github.com/rs/zerolog"
	"github.com/sausheong/mindb/src/server/internal/api"
)

// RecoveryMiddleware recovers from panics and logs stack traces
func RecoveryMiddleware(logger zerolog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			defer func() {
				if err := recover(); err != nil {
					// Log panic with stack trace
					logger.Error().
						Str("method", r.Method).
						Str("path", r.URL.Path).
						Str("remote_addr", r.RemoteAddr).
						Interface("panic", err).
						Bytes("stack", debug.Stack()).
						Msg("panic_recovered")

					// Return 500 error
					w.Header().Set("Content-Type", "application/json")
					w.WriteHeader(http.StatusInternalServerError)
					
					errResp := api.ErrorResponse{
						Error: api.ErrorDetail{
							Code:    api.ErrCodeInternal,
							Message: fmt.Sprintf("internal server error: %v", err),
						},
					}
					
					// Best effort write
					w.Write([]byte(fmt.Sprintf(`{"error":{"code":"%s","message":"%s"}}`, 
						errResp.Error.Code, errResp.Error.Message)))
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}
