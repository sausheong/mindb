package middleware

import (
	"fmt"
	"net/http"
)

// SecurityHeadersMiddleware adds security headers to all responses
func SecurityHeadersMiddleware() func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Prevent MIME type sniffing
			w.Header().Set("X-Content-Type-Options", "nosniff")
			
			// Prevent clickjacking
			w.Header().Set("X-Frame-Options", "DENY")
			
			// Enable XSS protection
			w.Header().Set("X-XSS-Protection", "1; mode=block")
			
			// Strict Transport Security (HSTS) - only if using HTTPS
			if r.TLS != nil {
				// max-age=31536000 (1 year), includeSubDomains, preload
				w.Header().Set("Strict-Transport-Security", "max-age=31536000; includeSubDomains; preload")
			}
			
			// Content Security Policy
			w.Header().Set("Content-Security-Policy", "default-src 'self'; script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'; img-src 'self' data:; font-src 'self'; connect-src 'self'; frame-ancestors 'none'")
			
			// Referrer Policy
			w.Header().Set("Referrer-Policy", "strict-origin-when-cross-origin")
			
			// Permissions Policy (formerly Feature-Policy)
			w.Header().Set("Permissions-Policy", "geolocation=(), microphone=(), camera=()")
			
			next.ServeHTTP(w, r)
		})
	}
}

// HTTPSRedirectMiddleware redirects HTTP requests to HTTPS
func HTTPSRedirectMiddleware(httpPort int) func(next http.Handler) http.Handler {
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// If already HTTPS, continue
			if r.TLS != nil {
				next.ServeHTTP(w, r)
				return
			}
			
			// Check if this is the HTTP port that should be redirected
			// If httpPort is 0, don't redirect (disabled)
			if httpPort == 0 {
				next.ServeHTTP(w, r)
				return
			}
			
			// Build HTTPS URL
			host := r.Host
			if httpPort != 80 {
				// Remove port from host if it matches the HTTP port
				host = r.Host
			}
			
			httpsURL := fmt.Sprintf("https://%s%s", host, r.RequestURI)
			
			// Permanent redirect (301)
			http.Redirect(w, r, httpsURL, http.StatusMovedPermanently)
		})
	}
}
