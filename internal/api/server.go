package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/redis/go-redis/v9"
)

// Server is the REST API server for pipeline mode.
type Server struct {
	db      *sql.DB
	redis   *redis.Client
	auth    *Auth
	dokploy *DokployClient
	workers map[string]*WorkerInfo // composeID → worker info
	mux     *http.ServeMux
	server  *http.Server
}

// NewServer creates an API server.
func NewServer(db *sql.DB, redisClient *redis.Client, authCfg AuthConfig, dokployCfg DokployConfig) *Server {
	s := &Server{
		db:      db,
		redis:   redisClient,
		auth:    NewAuth(authCfg),
		dokploy: NewDokployClient(dokployCfg),
		workers: make(map[string]*WorkerInfo),
		mux:     http.NewServeMux(),
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

	// Domains.
	s.mux.HandleFunc("GET /api/domains", RequireRole(RoleViewer, s.handleListDomains))

	// Categories.
	s.mux.HandleFunc("GET /api/categories", RequireRole(RoleViewer, s.handleListCategories))

	// Queries.
	s.mux.HandleFunc("GET /api/queries", RequireRole(RoleViewer, s.handleListQueries))

	// Pipeline.
	s.mux.HandleFunc("GET /api/pipeline/stats", RequireRole(RoleViewer, s.handlePipelineStats))

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
	where := "WHERE 1=1"
	args := []any{}
	argIdx := 1

	if domain := q.Get("domain"); domain != "" {
		where += fmt.Sprintf(" AND domain = $%d", argIdx)
		args = append(args, domain)
		argIdx++
	}
	if email := q.Get("email"); email != "" {
		where += fmt.Sprintf(" AND email ILIKE $%d", argIdx)
		args = append(args, "%"+email+"%")
		argIdx++
	}
	if hasEmail := q.Get("has_email"); hasEmail == "true" {
		where += " AND email IS NOT NULL AND email != ''"
	}

	// Count total.
	var total int
	countQuery := fmt.Sprintf("SELECT COUNT(*) FROM contacts %s", where)
	s.db.QueryRow(countQuery, args...).Scan(&total)

	// Fetch rows.
	dataQuery := fmt.Sprintf(`
		SELECT id, COALESCE(email,''), COALESCE(phone,''), domain, source_url,
		       COALESCE(instagram,''), COALESCE(facebook,''), COALESCE(twitter,''),
		       COALESCE(linkedin,''), COALESCE(whatsapp,''), COALESCE(address,''),
		       created_at
		FROM contacts %s
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
		var id int64
		var email, phone, domain, sourceURL string
		var instagram, facebook, twitter, linkedin, whatsapp, address string
		var createdAt time.Time

		rows.Scan(&id, &email, &phone, &domain, &sourceURL,
			&instagram, &facebook, &twitter, &linkedin, &whatsapp, &address,
			&createdAt)

		contacts = append(contacts, map[string]any{
			"id": id, "email": email, "phone": phone, "domain": domain,
			"source_url": sourceURL, "instagram": instagram, "facebook": facebook,
			"twitter": twitter, "linkedin": linkedin, "whatsapp": whatsapp,
			"address": address, "created_at": createdAt,
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

	where := ""
	if q.Get("has_email") == "true" {
		where = "WHERE email IS NOT NULL AND email != ''"
	}

	query := fmt.Sprintf(`
		SELECT COALESCE(email,''), COALESCE(phone,''), domain, source_url,
		       COALESCE(instagram,''), COALESCE(facebook,''), COALESCE(twitter,''),
		       COALESCE(linkedin,''), COALESCE(whatsapp,''), COALESCE(address,'')
		FROM contacts %s ORDER BY id ASC
	`, where)

	rows, err := s.db.Query(query)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	if format == "csv" {
		w.Header().Set("Content-Type", "text/csv")
		w.Header().Set("Content-Disposition", "attachment; filename=contacts.csv")
		fmt.Fprintln(w, "email,phone,domain,source_url,instagram,facebook,twitter,linkedin,whatsapp,address")
		for rows.Next() {
			var email, phone, domain, sourceURL string
			var ig, fb, tw, li, wa, addr string
			rows.Scan(&email, &phone, &domain, &sourceURL, &ig, &fb, &tw, &li, &wa, &addr)
			fmt.Fprintf(w, "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s\n",
				csvEscape(email), csvEscape(phone), csvEscape(domain), csvEscape(sourceURL),
				csvEscape(ig), csvEscape(fb), csvEscape(tw), csvEscape(li), csvEscape(wa), csvEscape(addr))
		}
		return
	}

	// JSON export.
	var contacts []map[string]string
	for rows.Next() {
		var email, phone, domain, sourceURL string
		var ig, fb, tw, li, wa, addr string
		rows.Scan(&email, &phone, &domain, &sourceURL, &ig, &fb, &tw, &li, &wa, &addr)
		contacts = append(contacts, map[string]string{
			"email": email, "phone": phone, "domain": domain, "source_url": sourceURL,
			"instagram": ig, "facebook": fb, "twitter": tw, "linkedin": li, "whatsapp": wa, "address": addr,
		})
	}
	writeJSON(w, http.StatusOK, contacts)
}

func (s *Server) handleContactStats(w http.ResponseWriter, r *http.Request) {
	var total, withEmail, uniqueDomains, uniqueEmails, lastHour, last24h int

	s.db.QueryRow("SELECT COUNT(*) FROM contacts").Scan(&total)
	s.db.QueryRow("SELECT COUNT(*) FROM contacts WHERE email IS NOT NULL AND email != ''").Scan(&withEmail)
	s.db.QueryRow("SELECT COUNT(DISTINCT domain) FROM contacts").Scan(&uniqueDomains)
	s.db.QueryRow("SELECT COUNT(DISTINCT email) FROM contacts WHERE email IS NOT NULL AND email != ''").Scan(&uniqueEmails)
	s.db.QueryRow("SELECT COUNT(*) FROM contacts WHERE created_at > NOW() - INTERVAL '1 hour'").Scan(&lastHour)
	s.db.QueryRow("SELECT COUNT(*) FROM contacts WHERE created_at > NOW() - INTERVAL '24 hours'").Scan(&last24h)

	writeJSON(w, http.StatusOK, map[string]int{
		"total":          total,
		"with_email":     withEmail,
		"unique_domains": uniqueDomains,
		"unique_emails":  uniqueEmails,
		"last_hour":      lastHour,
		"last_24h":       last24h,
	})
}

// ── Domain handlers ──

func (s *Server) handleListDomains(w http.ResponseWriter, r *http.Request) {
	rows, err := s.db.Query(`
		SELECT domain, COUNT(*) as contact_count,
		       COUNT(CASE WHEN email IS NOT NULL AND email != '' THEN 1 END) as email_count
		FROM contacts
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
	rows, err := s.db.Query(`
		SELECT id, text, status, result_count, COALESCE(error_msg,''), created_at
		FROM queries ORDER BY id DESC LIMIT 100
	`)
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
	writeJSON(w, http.StatusOK, queries)
}

// ── Pipeline stats ──

func (s *Server) handlePipelineStats(w http.ResponseWriter, r *http.Request) {
	ctx := context.Background()
	stats := map[string]any{}

	// Table counts.
	tables := map[string]string{
		"queries": "queries", "seeds": "serp_seeds",
		"websites": "websites", "contacts": "contacts",
	}
	for key, table := range tables {
		var total int
		s.db.QueryRow(fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&total)
		stats[key+"_total"] = total
	}

	// Queue depths from Redis.
	queues := []string{"serp:queue:websites", "serp:queue:contacts"}
	queueStats := map[string]int64{}
	for _, q := range queues {
		depth, _ := s.redis.ZCard(ctx, q).Result()
		queueStats[q] = depth
	}
	stats["queues"] = queueStats

	// Dedup sizes.
	dedups := []string{"serp:dedup:urls", "serp:dedup:domains", "serp:dedup:emails"}
	dedupStats := map[string]int64{}
	for _, d := range dedups {
		size, _ := s.redis.SCard(ctx, d).Result()
		dedupStats[d] = size
	}
	stats["dedup"] = dedupStats

	writeJSON(w, http.StatusOK, stats)
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
