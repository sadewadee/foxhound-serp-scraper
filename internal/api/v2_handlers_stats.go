package api

import (
	"context"
	"log/slog"
	"net/http"
	"time"
)

// dashboardStatsCacheTTL is the Redis cache window for /api/v2/stats.
// Dashboard polling clients re-hit within this window for ~free.
const dashboardStatsCacheTTL = 30 * time.Second

// handleV2DashboardStats returns the full dashboard stats wrapped in V2 format.
// Backed by a 30s Redis cache with single-flight to prevent thundering herd
// when the cache TTL expires under polling load.
func (s *Server) handleV2DashboardStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

	s.cachedJSON(ctx, w,
		"v2:stats:dashboard", dashboardStatsCacheTTL, 5*time.Second,
		func() (any, error) {
			return s.computeDashboardStats(ctx)
		},
	)
}

// computeDashboardStats runs the dashboard COUNT(*) queries against the live DB.
// Wrapped by cachedJSON above. The V2 envelope {"data": ...} is constructed
// inline because cachedJSON caches raw JSON bytes; going through writeV2Single
// inside the cached path would re-wrap on every cache miss.
func (s *Server) computeDashboardStats(ctx context.Context) (map[string]any, error) {
	// -- Queries (small table, no timeout needed) --
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

	// -- Big-table aggregates: serp_jobs + enrichment_jobs + emails --
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		slog.Error("v2: dashboard tx error", "error", err)
		return nil, err
	}
	defer tx.Rollback()

	// 15s — serp_jobs (5.7M rows) and emails (828K+) need headroom.
	if _, err := tx.ExecContext(ctx, "SET LOCAL statement_timeout = '15000'"); err != nil {
		slog.Error("v2: set timeout error", "error", err)
		return nil, err
	}

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

	queueMap := map[string]int64{}
	if s.redis != nil {
		qd, _ := s.redis.ZCard(ctx, "serp:queue:queries").Result()
		queueMap["serp:queue:queries"] = qd
		sb, _ := s.redis.LLen(ctx, "serp:buffer").Result()
		queueMap["serp:buffer"] = sb
		eb, _ := s.redis.LLen(ctx, "enrich:buffer").Result()
		queueMap["enrich:buffer"] = eb
	}

	// Wrap in V2 envelope manually since cachedJSON caches the raw bytes —
	// returning the inner object alone would force callers to re-wrap on hit.
	return map[string]any{
		"data": map[string]any{
			"queries": queries,
			"serp":    serp,
			"enrich":  enrich,
			"results": results,
			"queues":  queueMap,
		},
	}, nil
}
