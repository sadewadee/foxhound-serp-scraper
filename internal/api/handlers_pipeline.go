package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	pq "github.com/lib/pq"
)

func (s *Server) handlePipelineStats(w http.ResponseWriter, r *http.Request) {
	// 10s budget — must exceed the 8s statement_timeout below so PG cancels
	// the query first and we observe a clean error rather than a context-cancel.
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	stats := map[string]any{}

	// Wrap big-table COUNT(*) in a single tx with statement_timeout so a slow
	// query can't hold the manager's 2-conn pool open indefinitely (the cascade
	// that was causing v2 endpoints to balas 500 with context deadline exceeded).
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v1: pipeline-stats tx error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '8000'"); err != nil {
		slog.Error("v1: pipeline-stats set timeout error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	// Table counts. tables is compile-time only — no user input flows into the
	// SQL string here, so fmt.Sprintf is safe.
	tables := map[string]string{
		"queries": "queries", "serp_jobs": "serp_jobs",
		"enrichment_jobs": "enrichment_jobs", "emails": "emails",
		"business_listings": "business_listings",
	}
	for key, table := range tables {
		var total int
		if err := tx.QueryRowContext(ctx, fmt.Sprintf("SELECT COUNT(*) FROM %s", table)).Scan(&total); err != nil {
			slog.Warn("v1: pipeline-stats count failed", "table", table, "error", err)
		}
		stats[key+"_total"] = total
	}
	tx.Commit()

	// Queue depths from Redis (buffers are LISTs, queries queue is sorted set).
	queueStats := map[string]int64{}
	qDepth, _ := s.redis.ZCard(ctx, "serp:queue:queries").Result()
	queueStats["serp:queue:queries"] = qDepth
	serpBuf, _ := s.redis.LLen(ctx, "serp:buffer").Result()
	queueStats["serp:buffer"] = serpBuf
	enrichBuf, _ := s.redis.LLen(ctx, "enrich:buffer").Result()
	queueStats["enrich:buffer"] = enrichBuf
	stats["queues"] = queueStats

	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// 18s — same budget as v2_response.v2RequestContext so the V1 dashboard
	// won't outlive V2 requests. Statement timeout below is 15s; PG cancels first.
	ctx, cancel := context.WithTimeout(r.Context(), 18*time.Second)
	defer cancel()

	// -- Queries (small table — runs outside the big tx since CSV-style status
	// rollup is cheap and we want it to complete even if the tx hits timeout.) --
	queries := map[string]int{}
	qRows, _ := s.db.QueryContext(ctx, `SELECT status, COUNT(*) FROM queries GROUP BY status`)
	if qRows != nil {
		for qRows.Next() {
			var status string
			var cnt int
			qRows.Scan(&status, &cnt)
			queries[status] = cnt
		}
		qRows.Close()
	}
	qTotal := 0
	for _, c := range queries {
		qTotal += c
	}
	queries["total"] = qTotal

	// Big-table aggregates wrapped in a tx + statement_timeout to bound the
	// time these can hold the manager's 2-conn pool. Without this V2 endpoints
	// see 500 with context deadline exceeded when this handler runs slow.
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v1: dashboard tx error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '15000'"); err != nil {
		slog.Error("v1: dashboard set timeout error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	// -- SERP Jobs --
	serp := map[string]any{}
	var serpTotal, serpNew, serpProcessing, serpCompleted, serpFailed int
	var serpURLsFound, serpPerHour, serpToday int
	tx.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'new'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COALESCE(SUM(result_count) FILTER (WHERE status = 'completed'), 0),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '24 hours')
		FROM serp_jobs
	`).Scan(&serpTotal, &serpNew, &serpProcessing, &serpCompleted, &serpFailed, &serpURLsFound, &serpPerHour, &serpToday)
	serp["total"] = serpTotal
	serp["pending"] = serpNew
	serp["processing"] = serpProcessing
	serp["completed"] = serpCompleted
	serp["failed"] = serpFailed
	serp["urls_found"] = serpURLsFound
	serp["rate_per_hour"] = serpPerHour
	serp["today"] = serpToday

	// -- Enrich Jobs --
	enrich := map[string]any{}
	var enrichTotal, enrichPending, enrichProcessing, enrichCompleted, enrichFailed, enrichDead int
	var enrichPerHour, enrichToday int
	tx.QueryRowContext(ctx, `
		SELECT
			COUNT(*),
			COUNT(*) FILTER (WHERE status = 'pending'),
			COUNT(*) FILTER (WHERE status = 'processing'),
			COUNT(*) FILTER (WHERE status = 'completed'),
			COUNT(*) FILTER (WHERE status = 'failed'),
			COUNT(*) FILTER (WHERE status = 'dead'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'),
			COUNT(*) FILTER (WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours')
		FROM enrichment_jobs
	`).Scan(&enrichTotal, &enrichPending, &enrichProcessing, &enrichCompleted, &enrichFailed, &enrichDead, &enrichPerHour, &enrichToday)
	enrich["total"] = enrichTotal
	enrich["pending"] = enrichPending
	enrich["processing"] = enrichProcessing
	enrich["completed"] = enrichCompleted
	enrich["failed"] = enrichFailed
	enrich["dead"] = enrichDead
	enrich["rate_per_hour"] = enrichPerHour
	enrich["today"] = enrichToday

	// -- Contacts (from normalized tables) --
	contacts := map[string]any{}
	var totalEmails, uniqueEmails, emailsToday, emailsLastHour, uniqueDomains int
	tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM emails`).Scan(&totalEmails)
	uniqueEmails = totalEmails // emails table has UNIQUE constraint, so total == unique
	tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '24 hours'`).Scan(&emailsToday)
	tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '1 hour'`).Scan(&emailsLastHour)
	tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT domain) FROM business_listings`).Scan(&uniqueDomains)

	contacts["total_emails"] = totalEmails
	contacts["unique_emails"] = uniqueEmails
	contacts["emails_today"] = emailsToday
	contacts["emails_per_hour"] = emailsLastHour

	// Top email providers.
	providerRows, _ := tx.QueryContext(ctx, `
		SELECT domain, COUNT(*) AS cnt
		FROM emails
		GROUP BY domain ORDER BY cnt DESC LIMIT 10
	`)
	providers := map[string]int{}
	if providerRows != nil {
		for providerRows.Next() {
			var p string
			var c int
			providerRows.Scan(&p, &c)
			providers[p] = c
		}
		providerRows.Close()
	}
	contacts["providers"] = providers
	contacts["unique_domains"] = uniqueDomains

	tx.Commit()

	// -- Queues (Redis) --
	queueMap := map[string]int64{}
	qd, _ := s.redis.ZCard(ctx, "serp:queue:queries").Result()
	queueMap["serp:queue:queries"] = qd
	sb, _ := s.redis.LLen(ctx, "serp:buffer").Result()
	queueMap["serp:buffer"] = sb
	eb, _ := s.redis.LLen(ctx, "enrich:buffer").Result()
	queueMap["enrich:buffer"] = eb

	writeJSON(w, http.StatusOK, map[string]any{
		"queries":  queries,
		"serp":     serp,
		"enrich":   enrich,
		"contacts": contacts,
		"queues":   queueMap,
	})
}

func (s *Server) handlePipelineReset(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Queues bool `json:"queues"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "invalid request body"})
		return
	}
	if !req.Queues {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "specify queues to reset"})
		return
	}
	ctx := context.Background()
	cleared := map[string]int64{}
	// Clear buffers.
	for _, key := range []string{"serp:buffer", "enrich:buffer"} {
		n, _ := s.redis.LLen(ctx, key).Result()
		s.redis.Del(ctx, key)
		cleared[key] = n
	}
	// Clear query queue.
	n, _ := s.redis.ZCard(ctx, "serp:queue:queries").Result()
	s.redis.Del(ctx, "serp:queue:queries")
	cleared["serp:queue:queries"] = n

	writeJSON(w, http.StatusOK, map[string]any{"cleared": cleared})
}

// -- Debug --

func (s *Server) handleDebugSerpJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	parentID := q.Get("query_id")
	limit := queryInt(q, "limit", 50)

	where := "WHERE 1=1"
	args := []any{}
	argIdx := 1

	if parentID != "" {
		where += fmt.Sprintf(" AND parent_job_id = $%d", argIdx)
		args = append(args, parentID)
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT id, parent_job_id, search_url, page_num, status,
		       attempt_count, max_attempts, COALESCE(error_msg,''),
		       result_count, created_at, updated_at
		FROM serp_jobs %s
		ORDER BY parent_job_id, page_num
		LIMIT $%d
	`, where, argIdx)
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer rows.Close()

	var jobs []map[string]any
	for rows.Next() {
		var id string
		var parentJobID int64
		var searchURL, status, errorMsg string
		var pageNum, attemptCount, maxAttempts, resultCount int
		var createdAt, updatedAt time.Time
		rows.Scan(&id, &parentJobID, &searchURL, &pageNum, &status,
			&attemptCount, &maxAttempts, &errorMsg, &resultCount, &createdAt, &updatedAt)
		jobs = append(jobs, map[string]any{
			"id": id, "parent_job_id": parentJobID, "search_url": searchURL,
			"page_num": pageNum, "status": status, "attempt_count": attemptCount,
			"max_attempts": maxAttempts, "error": errorMsg, "result_count": resultCount,
			"created_at": createdAt, "updated_at": updatedAt,
		})
	}
	writeJSON(w, http.StatusOK, jobs)
}

func (s *Server) handleDebugEnrichJobs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	status := q.Get("status")
	limit := queryInt(q, "limit", 50)

	where := "WHERE 1=1"
	args := []any{}
	argIdx := 1

	if status != "" {
		where += fmt.Sprintf(" AND status = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	query := fmt.Sprintf(`
		SELECT id, domain, url, status, attempt_count, max_attempts,
		       COALESCE(error_msg,''), raw_emails, raw_phones, created_at, updated_at
		FROM enrichment_jobs %s
		ORDER BY created_at DESC
		LIMIT $%d
	`, where, argIdx)
	args = append(args, limit)

	rows, err := s.db.Query(query, args...)
	if err != nil {
		slog.Error("handler error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	defer rows.Close()

	var jobs []map[string]any
	for rows.Next() {
		var id, domain, url, jobStatus, errorMsg string
		var attemptCount, maxAttempts int
		var emails, phones []string
		var createdAt, updatedAt time.Time
		rows.Scan(&id, &domain, &url, &jobStatus, &attemptCount, &maxAttempts,
			&errorMsg, pq.Array(&emails), pq.Array(&phones), &createdAt, &updatedAt)
		jobs = append(jobs, map[string]any{
			"id": id, "domain": domain, "url": url, "status": jobStatus,
			"attempt_count": attemptCount, "max_attempts": maxAttempts,
			"error": errorMsg, "emails": emails, "phones": phones,
			"created_at": createdAt, "updated_at": updatedAt,
		})
	}
	writeJSON(w, http.StatusOK, jobs)
}

// -- Health --

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	health := map[string]string{"status": "ok"}

	if err := s.db.Ping(); err != nil {
		slog.Warn("health: postgres error", "error", err)
		health["postgres"] = "error"
		health["status"] = "degraded"
	} else {
		health["postgres"] = "ok"
	}

	if err := s.redis.Ping(context.Background()).Err(); err != nil {
		slog.Warn("health: redis error", "error", err)
		health["redis"] = "error"
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
