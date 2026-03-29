package api

import (
	"context"
	"database/sql"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
	"github.com/sadewadee/serp-scraper/internal/query"
)

// Server is the REST API server for pipeline mode.
type Server struct {
	db        *sql.DB
	redis     *redis.Client
	auth      *Auth
	mux       *http.ServeMux
	server    *http.Server
	queryRepo *query.Repository
}

// NewServer creates an API server.
func NewServer(db *sql.DB, redisClient *redis.Client, authCfg AuthConfig) *Server {
	s := &Server{
		db:        db,
		redis:     redisClient,
		auth:      NewAuth(authCfg),
		mux:       http.NewServeMux(),
		queryRepo: query.NewRepositoryWithRedis(db, redisClient),
	}
	s.registerRoutes()
	return s
}

func (s *Server) registerRoutes() {
	// Auth.
	s.mux.HandleFunc("POST /api/auth/login", s.handleLogin)
	s.mux.HandleFunc("POST /api/auth/refresh", s.handleRefresh)

	// Contacts.
	s.mux.HandleFunc("GET /api/contacts", RequireRole(RoleViewer, s.handleListContacts))
	s.mux.HandleFunc("GET /api/contacts/export", RequireRole(RoleViewer, s.handleExportContacts))
	s.mux.HandleFunc("GET /api/contacts/stats", RequireRole(RoleViewer, s.handleContactStats))
	s.mux.HandleFunc("DELETE /api/contacts", RequireRole(RoleAdmin, s.handleDeleteContacts))

	// Domains.
	s.mux.HandleFunc("GET /api/domains", RequireRole(RoleViewer, s.handleListDomains))

	// Categories.
	s.mux.HandleFunc("GET /api/categories", RequireRole(RoleViewer, s.handleListCategories))

	// Queries.
	s.mux.HandleFunc("GET /api/queries", RequireRole(RoleViewer, s.handleListQueries))
	s.mux.HandleFunc("POST /api/queries", RequireRole(RoleAdmin, s.handleCreateQueries))
	s.mux.HandleFunc("DELETE /api/queries", RequireRole(RoleAdmin, s.handleDeleteQueries))
	s.mux.HandleFunc("POST /api/queries/generate", RequireRole(RoleAdmin, s.handleGenerateQueries))
	s.mux.HandleFunc("POST /api/queries/retry", RequireRole(RoleAdmin, s.handleRetryQueries))
	s.mux.HandleFunc("GET /api/queries/stats", RequireRole(RoleViewer, s.handleQueryStats))

	// Pipeline.
	s.mux.HandleFunc("GET /api/pipeline/stats", RequireRole(RoleViewer, s.handlePipelineStats))
	s.mux.HandleFunc("GET /api/dashboard", RequireRole(RoleViewer, s.handleDashboard))
	s.mux.HandleFunc("POST /api/pipeline/reset", RequireRole(RoleAdmin, s.handlePipelineReset))

	// Debug.
	s.mux.HandleFunc("GET /api/debug/serp-jobs", RequireRole(RoleViewer, s.handleDebugSerpJobs))
	s.mux.HandleFunc("GET /api/debug/enrich-jobs", RequireRole(RoleViewer, s.handleDebugEnrichJobs))

	// Health.
	s.mux.HandleFunc("GET /api/health", s.handleHealth)
}

// Start begins serving the API.
func (s *Server) Start(addr string) error {
	s.server = &http.Server{
		Addr:         addr,
		Handler:      s.auth.AuthMiddleware(s.mux),
		ReadTimeout:  30 * time.Second,
		WriteTimeout: 60 * time.Second,
	}
	slog.Info("api: starting server", "addr", addr)
	return s.server.ListenAndServe()
}

// Shutdown gracefully stops the server.
func (s *Server) Shutdown(ctx context.Context) error {
	if s.server != nil {
		return s.server.Shutdown(ctx)
	}
	return nil
}
