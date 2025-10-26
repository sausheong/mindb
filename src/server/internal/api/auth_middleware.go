package api

import (
	"context"
	"net/http"
	"strings"

	"github.com/sausheong/mindb/src/server/internal/db"
)

// AuthMiddleware provides HTTP Basic Authentication
func AuthMiddleware(adapter *db.Adapter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Get credentials from Basic Auth
			username, password, ok := r.BasicAuth()
			if !ok {
				w.Header().Set("WWW-Authenticate", `Basic realm="mindb"`)
				http.Error(w, "Unauthorized - Authentication required", http.StatusUnauthorized)
				return
			}
			
			// Get client IP for host matching
			host := getClientIP(r)
			
			// Authenticate user
			if !adapter.Authenticate(username, password, host) {
				w.Header().Set("WWW-Authenticate", `Basic realm="mindb"`)
				http.Error(w, "Unauthorized - Invalid credentials", http.StatusUnauthorized)
				return
			}
			
			// Set current user in adapter
			adapter.SetCurrentUser(username, host)
			
			// Store user in context for logging
			ctx := context.WithValue(r.Context(), "user", username+"@"+host)
			
			// Continue to next handler
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}

// getClientIP extracts the client IP from the request
func getClientIP(r *http.Request) string {
	// Check X-Forwarded-For header (if behind proxy)
	forwarded := r.Header.Get("X-Forwarded-For")
	if forwarded != "" {
		// Take the first IP if multiple
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
	// Remove port if present
	if idx := strings.LastIndex(ip, ":"); idx != -1 {
		ip = ip[:idx]
	}
	
	// Use wildcard for localhost
	if ip == "127.0.0.1" || ip == "::1" {
		return "%"
	}
	
	return ip
}

// OptionalAuthMiddleware provides optional authentication (allows unauthenticated access)
func OptionalAuthMiddleware(adapter *db.Adapter) func(http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Try to get credentials
			username, password, ok := r.BasicAuth()
			
			if ok {
				host := getClientIP(r)
				
				// If credentials provided, validate them
				if adapter.Authenticate(username, password, host) {
					adapter.SetCurrentUser(username, host)
					ctx := context.WithValue(r.Context(), "user", username+"@"+host)
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
			}
			
			// No auth or invalid auth - use root user (backward compatibility)
			adapter.SetCurrentUser("root", "%")
			ctx := context.WithValue(r.Context(), "user", "root@%")
			next.ServeHTTP(w, r.WithContext(ctx))
		})
	}
}
