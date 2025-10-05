package middleware

import (
	"net/http"
	"strings"

	"github.com/sausheong/mindb/cmd/mindb-server/internal/api"
)

// AuthMiddleware validates API key or bearer token
func AuthMiddleware(apiKey string, disabled bool) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip auth if disabled
			if disabled {
				next.ServeHTTP(w, r)
				return
			}

			// Check X-API-Key header
			if key := r.Header.Get("X-API-Key"); key != "" {
				if key == apiKey {
					next.ServeHTTP(w, r)
					return
				}
			}

			// Check Authorization header
			if auth := r.Header.Get("Authorization"); auth != "" {
				if strings.HasPrefix(auth, "Bearer ") {
					token := strings.TrimPrefix(auth, "Bearer ")
					if token == apiKey {
						next.ServeHTTP(w, r)
						return
					}
				}
			}

			// Unauthorized
			writeError(w, http.StatusUnauthorized, api.ErrCodeUnauthorized, "invalid or missing API key")
		})
	}
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write([]byte(`{"error":{"code":"` + code + `","message":"` + message + `"}}`))
}
