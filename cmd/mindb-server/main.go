package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/go-chi/chi/v5"
	chimiddleware "github.com/go-chi/chi/v5/middleware"
	"github.com/rs/zerolog"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"

	"github.com/sausheong/mindb/cmd/mindb-server/internal/api"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/config"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/db"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/lockfile"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/middleware"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/semaphore"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/session"
	"github.com/sausheong/mindb/cmd/mindb-server/internal/txmanager"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	// Load configuration
	cfg, err := config.LoadFromEnv()
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	// Setup logger
	logger := setupLogger(cfg.LogLevel)
	logger.Info().
		Str("data_dir", cfg.DataDir).
		Str("http_addr", cfg.HTTPAddr).
		Int("exec_concurrency", cfg.ExecConcurrency).
		Msg("starting mindb-server")

	// Acquire lockfile
	lock, err := lockfile.Acquire(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("failed to acquire lock: %w", err)
	}
	defer func() {
		if err := lock.Release(); err != nil {
			logger.Error().Err(err).Msg("failed to release lock")
		}
	}()

	logger.Info().Msg("lockfile acquired")

	// Initialize database
	database, err := db.NewAdapter(cfg.DataDir)
	if err != nil {
		return fmt.Errorf("failed to initialize database: %w", err)
	}
	defer func() {
		logger.Info().Msg("closing database")
		if err := database.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close database")
		}
	}()

	logger.Info().Msg("database initialized")

	// Create transaction manager
	txMgr := txmanager.NewManager(cfg.TxIdleTimeout, cfg.MaxOpenTx, cfg.MaxTxPerClient)
	defer func() {
		logger.Info().Msg("closing transaction manager")
		if err := txMgr.Close(); err != nil {
			logger.Error().Err(err).Msg("failed to close transaction manager")
		}
	}()

	// Create execution semaphore
	execSem := semaphore.New(cfg.ExecConcurrency)

	// Create session manager
	sessionMgr := session.NewManager(cfg.SessionSigningKey, cfg.SessionTimeout)
	
	// Start session cleanup worker
	go middleware.SessionCleanupWorker(sessionMgr, 5*time.Minute)

	// Create handlers
	handlers := api.NewHandlers(database, txMgr, execSem, logger, cfg.StmtTimeout)

	// Setup HTTP router
	r := chi.NewRouter()

	// Global middleware
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(middleware.RecoveryMiddleware(logger))
	r.Use(middleware.LoggingMiddleware(logger))
	r.Use(middleware.SecurityHeadersMiddleware())
	r.Use(chimiddleware.Compress(5))

	// HTTPS redirect middleware (if TLS is enabled and redirect is configured)
	if cfg.EnableTLS && cfg.TLSRedirectHTTP {
		r.Use(middleware.HTTPSRedirectMiddleware(cfg.TLSRedirectPort))
	}

	// Auth middleware (if enabled)
	if !cfg.AuthDisabled {
		// Use session-based authentication with HTTP-only cookies
		r.Use(middleware.SessionAuthMiddleware(database, sessionMgr, true))
	} else {
		// No auth - use root user
		r.Use(middleware.SessionAuthMiddleware(database, sessionMgr, true))
	}

	// Routes
	r.Post("/query", handlers.QueryHandler())
	r.Post("/execute", handlers.ExecuteHandler())
	r.Post("/query/batch", handlers.BatchQueryHandler()) // Batch queries
	
	// Transaction routes
	r.Post("/tx/begin", handlers.TxBeginHandler())
	r.Post("/tx/{txID}/exec", handlers.TxExecHandler())
	r.Post("/tx/{txID}/commit", handlers.TxCommitHandler())
	r.Post("/tx/{txID}/rollback", handlers.TxRollbackHandler())
	
	// WASM Stored Procedure routes
	r.Post("/procedures", handlers.CreateProcedureHandler())        // Create procedure
	r.Delete("/procedures/{name}", handlers.DropProcedureHandler()) // Drop procedure
	r.Get("/procedures", handlers.ListProceduresHandler())          // List procedures
	r.Post("/procedures/{name}/call", handlers.CallProcedureHandler()) // Call procedure
	
	// Streaming
	r.Get("/stream", handlers.StreamHandler())
	
	// Health check
	r.Get("/health", handlers.HealthHandler())
	
	// Session management endpoints
	r.Post("/auth/logout", middleware.LogoutHandler(sessionMgr))
	r.Post("/auth/refresh", middleware.RefreshSessionHandler(sessionMgr))

	// Metrics endpoint (if enabled)
	if cfg.EnableMetrics {
		// TODO: Add Prometheus metrics handler
		// r.Get("/metrics", promhttp.Handler())
	}

	// Serve web console at /console
	r.Get("/console", func(w http.ResponseWriter, r *http.Request) {
		http.ServeFile(w, r, "./web/index.html")
	})
	
	// Serve static files for web console
	r.Get("/console/*", func(w http.ResponseWriter, r *http.Request) {
		// Remove /console prefix to get the actual file path
		filePath := strings.TrimPrefix(r.URL.Path, "/console")
		if filePath == "" || filePath == "/" {
			http.ServeFile(w, r, "./web/index.html")
			return
		}
		
		// Serve static file from web directory
		fullPath := "./web" + filePath
		http.ServeFile(w, r, fullPath)
	})
	
	// Create HTTP server with optimized settings
	var handler http.Handler
	if cfg.EnableTLS {
		// Use native HTTP/2 with TLS
		handler = r
	} else {
		// Use h2c (HTTP/2 without TLS) for development
		handler = h2c.NewHandler(r, &http2.Server{})
	}

	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      handler,
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
	}

	// Configure TLS if enabled
	if cfg.EnableTLS {
		tlsConfig := configureTLS(cfg)
		srv.TLSConfig = tlsConfig
		logger.Info().Msg("TLS 1.3 configured with secure cipher suites")
	}

	// Log server startup
	if cfg.EnableTLS {
		logger.Info().
			Str("addr", cfg.HTTPAddr).
			Str("cert", cfg.TLSCertFile).
			Msg("server listening with TLS")
	} else {
		logger.Info().
			Str("addr", cfg.HTTPAddr).
			Msg("server listening (HTTP - consider enabling TLS for production)")
	}
	
	// Configure transport for better connection pooling
	srv.SetKeepAlivesEnabled(true)
	if cfg.EnableTLS {
		logger.Info().Msg("HTTP/2 enabled with TLS")
	} else {
		logger.Info().Msg("HTTP/2 enabled (h2c - consider enabling TLS for production)")
	}

	// Start server in goroutine
	serverErrors := make(chan error, 1)
	go func() {
		if cfg.EnableTLS {
			// Start with TLS
			if cfg.TLSCertFile == "" || cfg.TLSKeyFile == "" {
				serverErrors <- fmt.Errorf("TLS enabled but cert/key files not specified")
				return
			}
			serverErrors <- srv.ListenAndServeTLS(cfg.TLSCertFile, cfg.TLSKeyFile)
		} else {
			// Start without TLS
			serverErrors <- srv.ListenAndServe()
		}
	}()

	// Wait for interrupt signal or server error
	shutdown := make(chan os.Signal, 1)
	signal.Notify(shutdown, os.Interrupt, syscall.SIGTERM)

	select {
	case err := <-serverErrors:
		return fmt.Errorf("server error: %w", err)

	case sig := <-shutdown:
		logger.Info().Str("signal", sig.String()).Msg("shutdown signal received")

		// Give outstanding requests time to complete
		ctx, cancel := context.WithTimeout(context.Background(), cfg.ShutdownGrace)
		defer cancel()

		// Shutdown HTTP server
		if err := srv.Shutdown(ctx); err != nil {
			logger.Error().Err(err).Msg("graceful shutdown failed")
			srv.Close()
			return fmt.Errorf("failed to stop server gracefully: %w", err)
		}

		logger.Info().Msg("server stopped gracefully")
	}

	return nil
}

func setupLogger(level string) zerolog.Logger {
	// Parse log level
	logLevel, err := zerolog.ParseLevel(level)
	if err != nil {
		logLevel = zerolog.InfoLevel
	}

	// Setup pretty console logging for development
	output := zerolog.ConsoleWriter{
		Out:        os.Stdout,
		TimeFormat: time.RFC3339,
	}

	return zerolog.New(output).
		Level(logLevel).
		With().
		Timestamp().
		Caller().
		Logger()
}

// configureTLS sets up TLS 1.3 with secure cipher suites and best practices
func configureTLS(cfg *config.Config) *tls.Config {
	return &tls.Config{
		// Minimum TLS version: TLS 1.3 (most secure)
		MinVersion: tls.VersionTLS13,
		
		// Prefer server cipher suites
		PreferServerCipherSuites: true,
		
		// TLS 1.3 cipher suites (automatically used with TLS 1.3)
		// TLS 1.3 has only secure cipher suites, no need to explicitly configure
		
		// Curve preferences for ECDHE
		CurvePreferences: []tls.CurveID{
			tls.X25519,    // Most secure and fastest
			tls.CurveP256, // NIST P-256
			tls.CurveP384, // NIST P-384
		},
		
		// Session tickets for resumption (TLS 1.3)
		SessionTicketsDisabled: false,
		
		// Client authentication (optional, can be configured later)
		ClientAuth: tls.NoClientCert,
		
		// Next protocols for HTTP/2
		NextProtos: []string{"h2", "http/1.1"},
	}
}
