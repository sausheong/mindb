package middleware

import (
	"net/http"
	"time"

	"github.com/rs/zerolog"
)

type responseWriter struct {
	http.ResponseWriter
	status int
	bytes  int
}

func (rw *responseWriter) WriteHeader(status int) {
	rw.status = status
	rw.ResponseWriter.WriteHeader(status)
}

func (rw *responseWriter) Write(b []byte) (int, error) {
	n, err := rw.ResponseWriter.Write(b)
	rw.bytes += n
	return n, err
}

// LoggingMiddleware logs HTTP requests with structured fields
func LoggingMiddleware(logger zerolog.Logger) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			start := time.Now()

			// Wrap response writer to capture status and bytes
			rw := &responseWriter{
				ResponseWriter: w,
				status:         http.StatusOK,
			}

			// Process request
			next.ServeHTTP(rw, r)

			// Log request
			duration := time.Since(start)
			
			event := logger.Info()
			if rw.status >= 400 {
				event = logger.Warn()
			}
			if rw.status >= 500 {
				event = logger.Error()
			}

			event.
				Str("method", r.Method).
				Str("path", r.URL.Path).
				Str("remote_addr", r.RemoteAddr).
				Int("status", rw.status).
				Int("bytes", rw.bytes).
				Dur("latency", duration).
				Int64("latency_ms", duration.Milliseconds()).
				Str("user_agent", r.UserAgent()).
				Msg("http_request")
		})
	}
}
