package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
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

	// Create handlers
	handlers := api.NewHandlers(database, txMgr, execSem, logger, cfg.StmtTimeout)

	// Setup HTTP router
	r := chi.NewRouter()

	// Global middleware
	r.Use(chimiddleware.RequestID)
	r.Use(chimiddleware.RealIP)
	r.Use(middleware.RecoveryMiddleware(logger))
	r.Use(middleware.LoggingMiddleware(logger))
	r.Use(chimiddleware.Compress(5))

	// Auth middleware (if enabled)
	if !cfg.AuthDisabled {
		r.Use(middleware.AuthMiddleware(cfg.APIKey, cfg.AuthDisabled))
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
	
	// Streaming
	r.Get("/stream", handlers.StreamHandler())
	
	// Health check
	r.Get("/health", handlers.HealthHandler())

	// Metrics endpoint (if enabled)
	if cfg.EnableMetrics {
		// TODO: Add Prometheus metrics handler
		// r.Get("/metrics", promhttp.Handler())
	}

	// Wrap handler with HTTP/2 support (h2c = HTTP/2 Cleartext)
	h2s := &http2.Server{}
	h2cHandler := h2c.NewHandler(r, h2s)
	
	// Create HTTP server with optimized settings
	srv := &http.Server{
		Addr:         cfg.HTTPAddr,
		Handler:      h2cHandler, // HTTP/2 enabled
		ReadTimeout:  cfg.ReadTimeout,
		WriteTimeout: cfg.WriteTimeout,
		IdleTimeout:  cfg.IdleTimeout,
		// Enable HTTP/2 and connection pooling
		MaxHeaderBytes: 1 << 20, // 1 MB
	}
	
	// Configure transport for better connection pooling
	srv.SetKeepAlivesEnabled(true)
	
	logger.Info().Msg("HTTP/2 enabled (h2c)")

	// Start server in goroutine
	serverErrors := make(chan error, 1)
	go func() {
		logger.Info().Str("addr", cfg.HTTPAddr).Msg("http server listening")
		serverErrors <- srv.ListenAndServe()
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
