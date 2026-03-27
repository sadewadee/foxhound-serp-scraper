package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	pq "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
	"github.com/sadewadee/serp-scraper/internal/query"
)

// Server is the REST API server for pipeline mode.
type Server struct {
	db        *sql.DB
	redis     *redis.Client
	auth      *Auth
	dokploy   *DokployClient
	workers   map[string]*WorkerInfo // composeID → worker info
	mux       *http.ServeMux
	server    *http.Server
	queryRepo *query.Repository
}

// NewServer creates an API server.
func NewServer(db *sql.DB, redisClient *redis.Client, authCfg AuthConfig, dokployCfg DokployConfig) *Server {
	s := &Server{
		db:        db,
		redis:     redisClient,
		auth:      NewAuth(authCfg),
		dokploy:   NewDokployClient(dokployCfg),
		workers:   make(map[string]*WorkerInfo),
		mux:       http.NewServeMux(),
		queryRepo: query.NewRepository(db),
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
	s.mux.HandleFunc("POST /api/queries/retry", RequireRole(RoleAdmin, s.handleRetryQueries))
	s.mux.HandleFunc("GET /api/queries/stats", RequireRole(RoleViewer, s.handleQueryStats))

	// Pipeline.
	s.mux.HandleFunc("GET /api/pipeline/stats", RequireRole(RoleViewer, s.handlePipelineStats))
	s.mux.HandleFunc("POST /api/pipeline/reset", RequireRole(RoleAdmin, s.handlePipelineReset))

	// Workers (Dokploy-managed).
	s.mux.HandleFunc("POST /api/workers/deploy", RequireRole(RoleAdmin, s.handleDeployWorker))
	s.mux.HandleFunc("GET /api/workers", RequireRole(RoleViewer, s.handleListWorkers))
	s.mux.HandleFunc("POST /api/workers/stop", RequireRole(RoleAdmin, s.handleStopWorker))
	s.mux.HandleFunc("POST /api/workers/start", RequireRole(RoleAdmin, s.handleStartWorker))
	s.mux.HandleFunc("DELETE /api/workers", RequireRole(RoleAdmin, s.handleDeleteWorker))

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

// ── Auth handlers ──

func (s *Server) handleLogin(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Username string `json:"username"`
		Password string `json:"password"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}

	user, err := s.auth.Authenticate(req.Username, req.Password)
	if err != nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "invalid credentials"})
		return
	}

	token, err := s.auth.GenerateToken(user.Username, user.Role, 24*time.Hour)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "token generation failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"expires_in": 86400,
		"user":       map[string]string{"username": user.Username, "role": string(user.Role)},
	})
}

func (s *Server) handleRefresh(w http.ResponseWriter, r *http.Request) {
	user := getUserFromContext(r.Context())
	if user == nil {
		writeJSON(w, http.StatusUnauthorized, map[string]string{"error": "not authenticated"})
		return
	}

	token, err := s.auth.GenerateToken(user.Username, user.Role, 24*time.Hour)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "token generation failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"token":      token,
		"expires_in": 86400,
	})
}

// ── Contact handlers ──

func (s *Server) handleListContacts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := queryInt(q, "page", 1)
	perPage := queryInt(q, "per_page", 50)
	if perPage > 200 {
		perPage = 200
	}
	offset := (page - 1) * perPage

	// Build WHERE clause from filters.
	where := "WHERE status = 'completed'"
	args := []any{}
	argIdx := 1

	if domain := q.Get("domain"); domain != "" {
		where += fmt.Sprintf(" AND domain = $%d", argIdx)
		args = append(args, domain)
		argIdx++
	}
	if hasEmail := q.Get("has_email"); hasEmail == "true" {
		where += " AND array_length(emails, 1) > 0"
	}
	if email := q.Get("email"); email != "" {
		where += fmt.Sprintf(" AND $%d = ANY(emails)", argIdx)
		args = append(args, email)
		argIdx++
	}

	// Count total.
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM enrich_jobs %s", where)
	s.db.QueryRow(countQuery, args...).Scan(&total)

	// Fetch rows.
	dataQuery := fmt.Sprintf(`
		SELECT id, emails, phones, domain, url, social_links, address, status, created_at
		FROM enrich_jobs %s
		ORDER BY id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := s.db.Query(dataQuery, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	var contacts []map[string]any
	for rows.Next() {
		var id string
		var emails, phones []string
		var domain, url, address, status string
		var socialLinksJSON []byte
		var createdAt time.Time

		rows.Scan(&id, pq.Array(&emails), pq.Array(&phones), &domain, &url, &socialLinksJSON, &address, &status, &createdAt)

		contacts = append(contacts, map[string]any{
			"id": id, "emails": emails, "phones": phones, "domain": domain,
			"url": url, "social_links": json.RawMessage(socialLinksJSON),
			"address": address, "status": status, "created_at": createdAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data":     contacts,
		"total":    total,
		"page":     page,
		"per_page": perPage,
	})
}

func (s *Server) handleExportContacts(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	format := q.Get("format")
	if format == "" {
		format = "json"
	}

	where := "WHERE status = 'completed'"
	if q.Get("has_email") == "true" {
		where += " AND array_length(emails, 1) > 0"
	}

	exportQuery := fmt.Sprintf(`
		SELECT emails, phones, domain, url, social_links, address
		FROM enrich_jobs %s ORDER BY id ASC
	`, where)

	rows, err := s.db.Query(exportQuery)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	if format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=contacts.csv")
		fmt.Fprintln(w, "emails,phones,domain,url,social_links,address")
		for rows.Next() {
			var emails, phones []string
			var domain, url, address string
			var socialLinksJSON []byte
			rows.Scan(pq.Array(&emails), pq.Array(&phones), &domain, &url, &socialLinksJSON, &address)
			emailsStr := strings.Join(emails, ";")
			phonesStr := strings.Join(phones, ";")
			fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s\n",
				csvEscape(emailsStr), csvEscape(phonesStr), csvEscape(domain), csvEscape(url),
				csvEscape(string(socialLinksJSON)), csvEscape(address))
		}
		return
	}

	// JSON export.
	var contacts []map[string]any
	for rows.Next() {
		var emails, phones []string
		var domain, url, address string
		var socialLinksJSON []byte
		rows.Scan(pq.Array(&emails), pq.Array(&phones), &domain, &url, &socialLinksJSON, &address)
		contacts = append(contacts, map[string]any{
			"emails": emails, "phones": phones, "domain": domain, "url": url,
			"social_links": json.RawMessage(socialLinksJSON), "address": address,
		})
	}
	writeJSON(w, http.StatusOK, contacts)
}

func (s *Server) handleContactStats(w http.ResponseWriter, r *http.Request) {
	var total, withEmail, uniqueDomains, uniqueEmails, lastHour, last24h int

	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed'").Scan(&total)
	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND array_length(emails, 1) > 0").Scan(&withEmail)
	s.db.QueryRow("SELECT COUNT(DISTINCT domain) FROM enrich_jobs WHERE status = 'completed'").Scan(&uniqueDomains)
	s.db.QueryRow("SELECT COUNT(DISTINCT e) FROM enrich_jobs, unnest(emails) AS e WHERE status = 'completed'").Scan(&uniqueEmails)
	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'").Scan(&lastHour)
	s.db.QueryRow("SELECT COUNT(*) FROM enrich_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours'").Scan(&last24h)

	writeJSON(w, http.StatusOK, map[string]int{
		"total":          total,
		"with_email":     withEmail,
		"unique_domains": uniqueDomains,
		"unique_emails":  uniqueEmails,
		"last_hour":      lastHour,
		"last_24h":       last24h,
	})
}

func (s *Server) handleDeleteContacts(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs    []string `json:"ids"`
		Domain string   `json:"domain"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if len(req.IDs) == 0 && req.Domain == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide ids or domain"})
		return
	}
	if len(req.IDs) > 0 && req.Domain != "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "provide ids or domain, not both"})
		return
	}
	if len(req.IDs) > 1000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 1000 ids per request"})
		return
	}
	var res sql.Result
	var err error
	if req.Domain != "" {
		res, err = s.db.Exec("DELETE FROM enrich_jobs WHERE domain = $1", req.Domain)
	} else {
		res, err = s.db.Exec("DELETE FROM enrich_jobs WHERE id = ANY($1)", pq.Array(req.IDs))
	}
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	n, _ := res.RowsAffected()
	writeJSON(w, http.StatusOK, map[string]any{"deleted": n})
}

// ── Domain handlers ──

func (s *Server) handleListDomains(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`
		SELECT domain, COUNT(*) as contact_count,
		       COUNT(CASE WHEN array_length(emails, 1) > 0 THEN 1 END) as email_count
		FROM enrich_jobs
		WHERE status = 'completed'
		GROUP BY domain
		ORDER BY contact_count DESC
		LIMIT 500
	`)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	var domains []map[string]any
	for rows.Next() {
		var domain string
		var contactCount, emailCount int
		rows.Scan(&domain, &contactCount, &emailCount)
		domains = append(domains, map[string]any{
			"domain": domain, "contacts": contactCount, "emails": emailCount,
		})
	}
	writeJSON(w, http.StatusOK, domains)
}

// ── Category handlers ──

func (s *Server) handleListCategories(w http.ResponseWriter, r *http.Request) {
	// Categories come from JSON-LD extraction — stored in a future column.
	// For now, aggregate from contacts if column exists.
	writeJSON(w, http.StatusOK, []any{})
}

// ── Query handlers ──

func (s *Server) handleListQueries(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	page := queryInt(q, "page", 1)
	perPage := queryInt(q, "per_page", 50)
	if perPage > 200 {
		perPage = 200
	}
	offset := (page - 1) * perPage

	where := "WHERE 1=1"
	args := []any{}
	argIdx := 1

	if status := q.Get("status"); status != "" {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}
	if search := q.Get("search"); search != "" {
		where += fmt.Sprintf(" AND text ILIKE $%d", argIdx)
		args = append(args, "%"+search+"%")
		argIdx++
	}

	var total int
	s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM queries %s", where), args...).Scan(&total)

	dataQuery := fmt.Sprintf(`
		SELECT id, text, status, result_count, COALESCE(error_msg,''), created_at
		FROM queries %s
		ORDER BY id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := s.db.Query(dataQuery, args...)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	var queries []map[string]any
	for rows.Next() {
		var id int64
		var text, status, errMsg string
		var resultCount int
		var createdAt time.Time
		rows.Scan(&id, &text, &status, &resultCount, &errMsg, &createdAt)
		queries = append(queries, map[string]any{
			"id": id, "text": text, "status": status,
			"result_count": resultCount, "error": errMsg, "created_at": createdAt,
		})
	}

	writeJSON(w, http.StatusOK, map[string]any{
		"data":     queries,
		"total":    total,
		"page":     page,
		"per_page": perPage,
	})
}

func (s *Server) handleCreateQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queries []string `json:"queries"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	var cleaned []string
	for _, q := range req.Queries {
		q = strings.TrimSpace(q)
		if q != "" {
			cleaned = append(cleaned, q)
		}
	}
	if len(cleaned) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "queries array is empty"})
		return
	}
	if len(cleaned) > 500 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 500 queries per request"})
		return
	}
	inserted, err := s.queryRepo.InsertBatch(cleaned)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusCreated, map[string]any{
		"inserted":   inserted,
		"duplicates": len(cleaned) - inserted,
		"total":      len(cleaned),
	})
}

func (s *Server) handleDeleteQueries(w http.ResponseWriter, r *http.Request) {
	var req struct {
		IDs []int64 `json:"ids"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || len(req.IDs) == 0 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "ids array is required"})
		return
	}
	if len(req.IDs) > 1000 {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "max 1000 ids per request"})
		return
	}
	deleted, err := s.queryRepo.DeleteByIDs(req.IDs)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"deleted": deleted})
}

func (s *Server) handleRetryQueries(w http.ResponseWriter, r *http.Request) {
	retried, err := s.queryRepo.RetryErrors()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"retried": retried})
}

func (s *Server) handleQueryStats(w http.ResponseWriter, r *http.Request) {
	counts, err := s.queryRepo.CountByStatus()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	total := 0
	for _, c := range counts {
		total += c
	}
	counts["total"] = total
	writeJSON(w, http.StatusOK, counts)
}

// ── Pipeline stats ──

func (s *Server) handlePipelineStats(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	stats := map[string]any{}

	// Table counts.
	tables := map[string]string{
		"queries": "queries", "serp_jobs": "serp_jobs",
		"websites": "websites", "enrich_jobs": "enrich_jobs",
	}
	for key, table := range tables {
		var total int
		s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&total)
		stats[key+"_total"] = total
	}

	// Queue depths from Redis.
	queues := []string{"serp:queue:websites", "serp:queue:enrich"}
	queueStats := map[string]int64{}
	for _, q := range queues {
		depth, _ := s.redis.ZCard(ctx, q).Result()
		queueStats[q] = depth
	}
	stats["queues"] = queueStats

	// Dedup sizes.
	dedups := []string{"serp:dedup:urls", "serp:dedup:domains"}
	dedupStats := map[string]int64{}
	for _, d := range dedups {
		size, _ := s.redis.SCard(ctx, d).Result()
		dedupStats[d] = size
	}
	stats["dedup"] = dedupStats

	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handlePipelineReset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Dedup  bool `json:"dedup"`
		Queues bool `json:"queues"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if !req.Dedup && !req.Queues {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "specify dedup and/or queues to reset"})
		return
	}
	ctx := context.Background()
	cleared := map[string]int64{}
	if req.Dedup {
		for _, key := range []string{"serp:dedup:urls", "serp:dedup:domains"} {
			n, _ := s.redis.SCard(ctx, key).Result()
			s.redis.Del(ctx, key)
			cleared[key] = n
		}
	}
	if req.Queues {
		for _, key := range []string{"serp:queue:websites", "serp:queue:enrich"} {
			n, _ := s.redis.ZCard(ctx, key).Result()
			s.redis.Del(ctx, key)
			cleared[key] = n
		}
	}
	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared})
}

// ── Worker management (Dokploy) ──

func (s *Server) handleDeployWorker(w http.ResponseWriter, r *http.Request) {
	if s.dokploy == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{
			"error": "Dokploy not configured (set dokploy.url and dokploy.api_key in config)",
		})
		return
	}

	var req WorkerDeployRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request: " + err.Error()})
		return
	}
	if req.Name == "" || req.EnvID == "" || req.ServerID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "name, environment_id, server_id are required"})
		return
	}
	if req.PostgresDSN == "" || req.RedisAddr == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "postgres_dsn, redis_addr are required"})
		return
	}

	info, err := s.dokploy.DeployWorker(req)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	s.workers[info.ComposeID] = info
	writeJSON(w, http.StatusOK, info)
}

func (s *Server) handleListWorkers(w http.ResponseWriter, r *http.Request) {
	list := make([]*WorkerInfo, 0, len(s.workers))
	for _, w := range s.workers {
		list = append(list, w)
	}
	writeJSON(w, http.StatusOK, list)
}

func (s *Server) handleStopWorker(w http.ResponseWriter, r *http.Request) {
	if s.dokploy == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Dokploy not configured"})
		return
	}

	var req struct {
		ComposeID string `json:"compose_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ComposeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "compose_id is required"})
		return
	}

	if err := s.dokploy.StopWorker(req.ComposeID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if info, ok := s.workers[req.ComposeID]; ok {
		info.Status = "stopped"
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "stopped"})
}

func (s *Server) handleStartWorker(w http.ResponseWriter, r *http.Request) {
	if s.dokploy == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Dokploy not configured"})
		return
	}

	var req struct {
		ComposeID string `json:"compose_id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.ComposeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "compose_id is required"})
		return
	}

	if err := s.dokploy.StartWorker(req.ComposeID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if info, ok := s.workers[req.ComposeID]; ok {
		info.Status = "running"
	}
	writeJSON(w, http.StatusOK, map[string]string{"status": "started"})
}

func (s *Server) handleDeleteWorker(w http.ResponseWriter, r *http.Request) {
	if s.dokploy == nil {
		writeJSON(w, http.StatusServiceUnavailable, map[string]string{"error": "Dokploy not configured"})
		return
	}

	composeID := r.URL.Query().Get("compose_id")
	if composeID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "compose_id query param required"})
		return
	}

	if err := s.dokploy.DeleteWorker(composeID); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	delete(s.workers, composeID)
	writeJSON(w, http.StatusOK, map[string]string{"status": "deleted"})
}

// ── Health ──

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]string{"status": "ok"}

	if err := s.db.Ping(); err != nil {
		health["postgres"] = "error: " + err.Error()
		health["status"] = "degraded"
	} else {
		health["postgres"] = "ok"
	}

	if err := s.redis.Ping(context.Background()).Err(); err != nil {
		health["redis"] = "error: " + err.Error()
		health["status"] = "degraded"
	} else {
		health["redis"] = "ok"
	}

	status := http.StatusOK
	if health["status"] != "ok" {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, health)
}
