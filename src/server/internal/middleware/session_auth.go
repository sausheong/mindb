package middleware

import (
	"context"
	"net/http"
	"time"

	"github.com/sausheong/mindb/src/server/internal/api"
	"github.com/sausheong/mindb/src/server/internal/db"
	"github.com/sausheong/mindb/src/server/internal/session"
)

const (
	SessionCookieName = "mindb_session"
	SessionContextKey = "session_claims"
)

// SessionAuthMiddleware provides session-based authentication with HTTP-only cookies
func SessionAuthMiddleware(adapter *db.Adapter, sessionMgr *session.Manager, optional bool) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Try to get session cookie
			cookie, err := r.Cookie(SessionCookieName)
			
			if err == nil && cookie.Value != "" {
				// Validate session token
				claims, err := sessionMgr.ValidateSession(cookie.Value)
				if err == nil {
					// Valid session - set user in adapter
					adapter.SetCurrentUser(claims.Username, claims.Host)
					
					// Store claims in context
					ctx := context.WithValue(r.Context(), SessionContextKey, claims)
					ctx = context.WithValue(ctx, "user", claims.Username+"@"+claims.Host)
					
					next.ServeHTTP(w, r.WithContext(ctx))
					return
				}
				
				// Invalid or expired session - clear cookie
				clearSessionCookie(w)
			}
			
			// No valid session - try Basic Auth as fallback
			username, password, ok := r.BasicAuth()
			
			if ok {
				// Get client IP for host matching
				host := getClientIP(r)
				
				// Authenticate user
				if adapter.Authenticate(username, password, host) {
					// Log successful login
					adapter.LogLoginSuccess(username, host)
					
					// Create new session
					token, err := sessionMgr.CreateSession(username, host)
					if err == nil {
						// Set session cookie
						setSessionCookie(w, token, r.TLS != nil)
					}
					
					// Set current user in adapter
					adapter.SetCurrentUser(username, host)
					
					// Store user in context
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

// setSessionCookie sets an HTTP-only, secure session cookie
func setSessionCookie(w http.ResponseWriter, token string, secure bool) {
	cookie := &http.Cookie{
		Name:     SessionCookieName,
		Value:    token,
		Path:     "/",
		HttpOnly: true,                    // Prevents JavaScript access
		Secure:   secure,                  // Only send over HTTPS
		SameSite: http.SameSiteStrictMode, // CSRF protection
		MaxAge:   15 * 60,                 // 15 minutes
	}
	http.SetCookie(w, cookie)
}

// clearSessionCookie removes the session cookie
func clearSessionCookie(w http.ResponseWriter) {
	cookie := &http.Cookie{
		Name:     SessionCookieName,
		Value:    "",
		Path:     "/",
		HttpOnly: true,
		Secure:   true,
		SameSite: http.SameSiteStrictMode,
		MaxAge:   -1, // Delete cookie
	}
	http.SetCookie(w, cookie)
}

// LogoutHandler handles user logout by revoking the session
func LogoutHandler(sessionMgr *session.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get session cookie
		cookie, err := r.Cookie(SessionCookieName)
		if err == nil && cookie.Value != "" {
			// Revoke session
			sessionMgr.RevokeSession(cookie.Value)
		}
		
		// Clear cookie
		clearSessionCookie(w)
		
		// Return success
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"logged_out"}`))
	}
}

// RefreshSessionHandler extends the current session
func RefreshSessionHandler(sessionMgr *session.Manager) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// Get current session cookie
		cookie, err := r.Cookie(SessionCookieName)
		if err != nil || cookie.Value == "" {
			writeError(w, http.StatusUnauthorized, api.ErrCodeUnauthorized, "no active session")
			return
		}
		
		// Refresh session
		newToken, err := sessionMgr.RefreshSession(cookie.Value)
		if err != nil {
			clearSessionCookie(w)
			writeError(w, http.StatusUnauthorized, api.ErrCodeUnauthorized, "session expired")
			return
		}
		
		// Set new session cookie
		setSessionCookie(w, newToken, r.TLS != nil)
		
		// Return success
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`{"status":"session_refreshed","expires_in":900}`))
	}
}

// SessionCleanupWorker periodically cleans up expired sessions
func SessionCleanupWorker(sessionMgr *session.Manager, interval time.Duration) {
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	
	for range ticker.C {
		sessionMgr.CleanupExpiredSessions()
	}
}
