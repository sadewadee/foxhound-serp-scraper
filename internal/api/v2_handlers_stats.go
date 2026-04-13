package api

import (
	"log/slog"
	"net/http"
)

// handleV2DashboardStats returns the full dashboard stats wrapped in V2 format.
func (s *Server) handleV2DashboardStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	// -- Queries --
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

	// -- SERP Jobs (with statement timeout) --
	serp := map[string]any{}
	var serpTotal, serpNew, serpProcessing, serpCompleted, serpFailed int
	var serpURLsFound, serpPerHour, serpToday int

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v2: dashboard tx error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch dashboard stats")
		return
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '5000'"); err != nil {
		slog.Error("v2: set timeout error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to set timeout")
		return
	}

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

	// -- Results --
	results := map[string]any{}
	var totalEmails, emailsToday, emailsLastHour, uniqueDomains int
	tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM emails`).Scan(&totalEmails)
	tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '24 hours'`).Scan(&emailsToday)
	tx.QueryRowContext(ctx, `SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '1 hour'`).Scan(&emailsLastHour)
	tx.QueryRowContext(ctx, `SELECT COUNT(DISTINCT domain) FROM business_listings`).Scan(&uniqueDomains)

	results["total_emails"] = totalEmails
	results["emails_today"] = emailsToday
	results["emails_per_hour"] = emailsLastHour
	results["unique_domains"] = uniqueDomains

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
	results["providers"] = providers

	tx.Commit()

	// -- Queues (Redis) --
	queueMap := map[string]int64{}
	qd, _ := s.redis.ZCard(ctx, "serp:queue:queries").Result()
	queueMap["serp:queue:queries"] = qd
	sb, _ := s.redis.LLen(ctx, "serp:buffer").Result()
	queueMap["serp:buffer"] = sb
	eb, _ := s.redis.LLen(ctx, "enrich:buffer").Result()
	queueMap["enrich:buffer"] = eb

	writeV2Single(w, map[string]any{
		"queries": queries,
		"serp":    serp,
		"enrich":  enrich,
		"results": results,
		"queues":  queueMap,
	})
}
