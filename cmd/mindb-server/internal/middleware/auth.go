package middleware

import (
	"context"
	"net/http"
	"strings"

	"github.com/sausheong/mindb/cmd/mindb-server/internal/api"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/db"
)

// AuthMiddleware validates API key, bearer token, or database user credentials
func AuthMiddleware(apiKey string, disabled bool) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Skip auth if disabled
			if disabled {
				next.ServeHTTP(w, r)
				return
			}

			// Check X-API-Key header (backward compatibility)
			if key := r.Header.Get("X-API-Key"); key != "" {
				if key == apiKey {
					next.ServeHTTP(w, r)
					return
				}
			}

			// Check Authorization header for Bearer token (backward compatibility)
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

// DBAuthMiddleware provides database user authentication via HTTP Basic Auth
func DBAuthMiddleware(adapter *db.Adapter, optional bool) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Try to get credentials from Basic Auth
			username, password, ok := r.BasicAuth()
			
			if ok {
				// Get client IP for host matching
				host := getClientIP(r)
				
				// Authenticate user
				if adapter.Authenticate(username, password, host) {
					// Log successful login
					adapter.LogLoginSuccess(username, host)
					
					// Set current user in adapter
					adapter.SetCurrentUser(username, host)
					
					// Store user in context for logging
					ctx := context.WithValue(r.Context(), "user", username+"@"+host)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				
				// Log failed login
				reason := "invalid_password"
				if adapter.IsAccountLocked(username, host) {
					reason = "account_locked"
				}
				adapter.LogLoginFailed(username, host, reason)
				
				// Invalid credentials
				if !optional {
					w.Header().Set("WWW-Authenticate", `Basic realm="mindb"`)
					writeError(w, http.StatusUnauthorized, api.ErrCodeUnauthorized, "invalid credentials or account locked")
					return
				}
			} else if !optional {
				// No credentials provided and auth is required
				w.Header().Set("WWW-Authenticate", `Basic realm="mindb"`)
				writeError(w, http.StatusUnauthorized, api.ErrCodeUnauthorized, "authentication required")
				return
			}
			
			// Optional auth: use root user if no credentials provided
			if optional {
				adapter.SetCurrentUser("root", "%")
				ctx := context.WithValue(r.Context(), "user", "root@%")
				next.ServeHTTP(w, r.WithContext(ctx))
				return
			}
			
			next.ServeHTTP(w, r)
		})
	}
}

// getClientIP extracts the client IP from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (if behind proxy)
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		ips := strings.Split(forwarded, ",")
		return strings.TrimSpace(ips[0])
	}
	
	// Check X-Real-IP header
	realIP := r.Header.Get("X-Real-IP")
	if realIP != "" {
		return realIP
	}
	
	// Fall back to RemoteAddr
	ip := r.RemoteAddr
	if idx := strings.LastIndex(ip, ":"); idx != -1 {
		ip = ip[:idx]
	}
	
	// Use wildcard for localhost
	if ip == "127.0.0.1" || ip == "::1" || ip == "" {
		return "%"
	}
	
	return ip
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write([]byte(`{"error":{"code":"` + code + `","message":"` + message + `"}}`))
}
