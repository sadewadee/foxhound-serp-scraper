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
	ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
	defer cancel()

	// Table totals are pg_class.reltuples estimates (Invariant #2: no COUNT(*)
	// over serp_jobs / enrichment_jobs). A table without an estimate is null and
	// listed in unknown_figures — never a fake 0.
	tables := map[string]string{
		"queries": "queries", "serp_jobs": "serp_jobs",
		"enrichment_jobs": "enrichment_jobs", "emails": "emails",
		"business_listings": "business_listings",
	}
	est, err := s.tableEstimates(ctx, "queries", "serp_jobs", "enrichment_jobs", "emails", "business_listings")
	if err != nil {
		slog.Warn("v1: pipeline-stats estimates failed", "error", err)
	}
	stats := map[string]any{}
	unknown := []string{}
	for key, table := range tables {
		if n, ok := est[table]; ok {
			stats[key+"_total"] = n
		} else {
			stats[key+"_total"] = nil
			unknown = append(unknown, key+"_total")
		}
	}
	if len(unknown) == len(tables) {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}
	stats["totals_estimated"] = true
	stats["unknown_figures"] = unknown
	stats["queues"] = s.queueDepths(ctx)

	writeJSON(w, http.StatusOK, stats)
}

func (s *Server) handleDashboard(w http.ResponseWriter, r *http.Request) {
	// 18s — same budget as v2_response.v2RequestContext so the V1 dashboard
	// won't outlive V2 requests.
	ctx, cancel := context.WithTimeout(r.Context(), 18*time.Second)
	defer cancel()

	st, err := s.computeDashboardStats(ctx)
	if err != nil {
		slog.Error("v1: dashboard stats error", "error", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal server error"})
		return
	}

	// v1 calls the results block "contacts" and adds unique_emails (the emails
	// table has a UNIQUE constraint, so total == unique).
	contacts := st.Results
	contacts["unique_emails"] = contacts["total_emails"]

	writeJSON(w, http.StatusOK, map[string]any{
		"queries":           st.Queries,
		"serp":              st.Serp,
		"enrich":            st.Enrich,
		"contacts":          contacts,
		"queues":            st.Queues,
		"unknown_figures":   renamePrefix(nonNil(st.Unknown), "results", "contacts"),
		"estimated_figures": renamePrefix(nonNil(st.Estimated), "results", "contacts"),
		"snapshot_at":       st.SnapshotAt,
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
