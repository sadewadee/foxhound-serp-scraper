package api

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"time"

	pq "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// Dashboard stats are split by cost (CLAUDE.md Invariant #2: no unbounded
// COUNT(*) on serp_jobs / enrichment_jobs, which exceed 1M rows):
//
//   - Request path (cheap, each figure its own statement with a short timeout):
//     table totals from pg_class.reltuples (estimates), rare-status counts via the
//     status indexes, and time-windowed counts on indexed timestamp columns.
//   - Background (manager-only RunSlowStatsLoop, generous timeout): figures that
//     need a full scan — serp/enrich "completed", SUM(result_count),
//     COUNT(DISTINCT domain), email providers. Stored in a Redis hash the request
//     path just reads.
//
// A figure that cannot be produced is reported as null and listed in
// unknown_figures; it is never silently 0.
const (
	statsFastTimeout   = 2 * time.Second
	statsSlowTimeout   = 120 * time.Second
	statsSlowInterval  = 15 * time.Minute
	statsSlowKey       = "stats:slow:v1"
	statsSlowKeyTTL    = time.Hour // outlives several missed refreshes, then figures go unknown
	statsSnapshotField = "computed_at"
)

// fastFigures are the per-request counts. Every query is index-backed or
// time-windowed; a failure only marks that one figure unknown.
var fastFigures = []struct{ key, sql string }{
	{"serp.pending", `SELECT COUNT(*) FROM serp_jobs WHERE status = 'new'`},
	{"serp.processing", `SELECT COUNT(*) FROM serp_jobs WHERE status = 'processing'`},
	{"serp.failed", `SELECT COUNT(*) FROM serp_jobs WHERE status = 'failed'`},
	{"serp.rate_per_hour", `SELECT COUNT(*) FROM serp_jobs WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '1 hour'`},
	{"serp.today", `SELECT COUNT(*) FROM serp_jobs WHERE status = 'completed' AND updated_at > NOW() - INTERVAL '24 hours'`},
	{"enrich.pending", `SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'pending'`},
	{"enrich.processing", `SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'processing'`},
	{"enrich.failed", `SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'failed'`},
	{"enrich.dead", `SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'dead'`},
	{"enrich.rate_per_hour", `SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '1 hour'`},
	{"enrich.today", `SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'completed' AND completed_at > NOW() - INTERVAL '24 hours'`},
	{"results.emails_per_hour", `SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '1 hour'`},
	{"results.emails_today", `SELECT COUNT(*) FROM emails WHERE created_at > NOW() - INTERVAL '24 hours'`},
}

// slowFigures are full scans: the single sanctioned exception to Invariant #2 —
// manager-only, own 120s timeout, 15 min cadence, never on the request path.
// slowFigures are computed by RunSlowStatsLoop. Values are stored as strings;
// "results.providers" holds a JSON object.
var slowFigures = []struct{ key, sql string }{
	{"serp.completed", `SELECT COUNT(*) FROM serp_jobs WHERE status = 'completed'`},
	{"serp.urls_found", `SELECT COALESCE(SUM(result_count), 0) FROM serp_jobs WHERE status = 'completed'`},
	{"enrich.completed", `SELECT COUNT(*) FROM enrichment_jobs WHERE status = 'completed'`},
	{"results.unique_domains", `SELECT COUNT(DISTINCT domain) FROM business_listings`},
}

const providersKey = "results.providers"

const providersSQL = `
	SELECT COALESCE(json_object_agg(domain, cnt), '{}'::json)::text FROM (
		SELECT domain, COUNT(*) AS cnt FROM emails
		GROUP BY domain ORDER BY cnt DESC LIMIT 10
	) t`

// statQuery runs one single-row query in its own transaction with its own
// statement_timeout, so one slow or failing figure cannot abort any other.
func statQuery(ctx context.Context, db *sql.DB, timeout time.Duration, query string, dest any) error {
	tx, err := db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%d'", timeout.Milliseconds())); err != nil {
		return err
	}
	if err := tx.QueryRowContext(ctx, query).Scan(dest); err != nil {
		return err
	}
	return tx.Commit()
}

// tableEstimates returns pg_class.reltuples per table. Tables that are missing
// or never analyzed (reltuples < 0) are absent from the map. Estimates are accurate to well
// under 1% with autovacuum on, and cost one catalog lookup.
func (s *Server) tableEstimates(ctx context.Context, tables ...string) (map[string]int64, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%d'", statsFastTimeout.Milliseconds())); err != nil {
		return nil, err
	}
	// to_regclass resolves against search_path, avoiding same-named tables in
	// other schemas; unknown names yield NULL and are skipped.
	rows, err := tx.QueryContext(ctx, `
		SELECT t.name, c.reltuples::bigint
		FROM unnest($1::text[]) AS t(name)
		JOIN pg_class c ON c.oid = to_regclass(t.name)`, pq.Array(tables))
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	out := map[string]int64{}
	for rows.Next() {
		var name string
		var n int64
		if err := rows.Scan(&name, &n); err != nil {
			return nil, err
		}
		if n >= 0 {
			out[name] = n
		}
	}
	return out, rows.Err()
}

// dashboardStats is the shared result behind /api/v2/stats, /api/dashboard and
// /api/pipeline/stats. Missing figures are nil.
type dashboardStats struct {
	Queries, Serp, Enrich, Results map[string]any
	Queues                         map[string]int64
	Unknown, Estimated             []string
	SnapshotAt                     any // RFC3339 time of the background snapshot, or nil
}

// computeDashboardStats gathers every figure independently. It returns an error
// only when no figure at all could be produced (so callers don't cache or serve
// an all-unknown response as if it were data).
func (s *Server) computeDashboardStats(ctx context.Context) (*dashboardStats, error) {
	st := &dashboardStats{
		Queries: map[string]any{}, Serp: map[string]any{},
		Enrich: map[string]any{}, Results: map[string]any{},
	}
	blocks := map[string]map[string]any{"serp": st.Serp, "enrich": st.Enrich, "results": st.Results}
	known := 0
	set := func(key string, v any) {
		block, field, _ := strings.Cut(key, ".")
		blocks[block][field] = v
		if v == nil {
			st.Unknown = append(st.Unknown, key)
		} else {
			known++
		}
	}

	// Table totals: estimates from the catalog.
	est, err := s.tableEstimates(ctx, "queries", "serp_jobs", "enrichment_jobs", "emails", "business_listings")
	if err != nil {
		slog.Warn("stats: table estimates failed", "error", err)
	}
	estimate := func(table, key string) {
		if n, ok := est[table]; ok {
			set(key, n)
			st.Estimated = append(st.Estimated, key)
		} else {
			set(key, nil)
		}
	}
	estimate("serp_jobs", "serp.total")
	estimate("enrichment_jobs", "enrich.total")
	estimate("emails", "results.total_emails")

	// Queries: exact per-status breakdown (bounded by its own timeout); on
	// failure fall back to the estimated total only.
	if err := s.queryStatusCounts(ctx, st.Queries); err != nil {
		slog.Warn("stats: queries status breakdown failed", "error", err)
		st.Queries = map[string]any{"total": nil}
		if n, ok := est["queries"]; ok {
			st.Queries["total"] = n
			st.Estimated = append(st.Estimated, "queries.total")
			known++
		}
		st.Unknown = append(st.Unknown, "queries.status_breakdown")
	} else {
		known++
	}

	for _, f := range fastFigures {
		var n int64
		if err := statQuery(ctx, s.db, statsFastTimeout, f.sql, &n); err != nil {
			slog.Warn("stats: figure failed", "figure", f.key, "error", err)
			set(f.key, nil)
			continue
		}
		set(f.key, n)
	}

	// Background snapshot.
	snap := s.readSlowSnapshot(ctx)
	for _, f := range slowFigures {
		if v, ok := snap[f.key]; ok {
			if n, err := strconv.ParseInt(v, 10, 64); err == nil {
				set(f.key, n)
				continue
			}
		}
		set(f.key, nil)
	}
	providers := map[string]int64(nil)
	if v, ok := snap[providersKey]; ok {
		if err := json.Unmarshal([]byte(v), &providers); err != nil {
			providers = nil
		}
	}
	if providers != nil {
		set(providersKey, providers)
	} else {
		set(providersKey, nil)
	}
	if ts, err := strconv.ParseInt(snap[statsSnapshotField], 10, 64); err == nil {
		st.SnapshotAt = time.Unix(ts, 0).UTC().Format(time.RFC3339)
	}

	if known == 0 {
		return nil, errors.New("stats: no figure could be computed")
	}

	st.Queues = s.queueDepths(ctx)
	return st, nil
}

// queryStatusCounts fills dst with per-status counts and "total" from queries.
func (s *Server) queryStatusCounts(ctx context.Context, dst map[string]any) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%d'", statsFastTimeout.Milliseconds())); err != nil {
		return err
	}
	rows, err := tx.QueryContext(ctx, `SELECT status, COUNT(*) FROM queries GROUP BY status`)
	if err != nil {
		return err
	}
	defer rows.Close()
	var total int64
	counts := map[string]int64{}
	for rows.Next() {
		var status string
		var n int64
		if err := rows.Scan(&status, &n); err != nil {
			return err
		}
		counts[status] = n
		total += n
	}
	if err := rows.Err(); err != nil {
		return err
	}
	for k, v := range counts {
		dst[k] = v
	}
	dst["total"] = total
	return nil
}

// queueDepths reads Redis queue sizes; errors leave a depth at 0 (Redis is
// transient coordination, not a stat of record).
func (s *Server) queueDepths(ctx context.Context) map[string]int64 {
	m := map[string]int64{}
	if s.redis == nil {
		return m
	}
	m["serp:queue:queries"], _ = s.redis.ZCard(ctx, "serp:queue:queries").Result()
	m["serp:buffer"], _ = s.redis.LLen(ctx, "serp:buffer").Result()
	m["enrich:buffer"], _ = s.redis.LLen(ctx, "enrich:buffer").Result()
	return m
}

// readSlowSnapshot returns the background-computed figures, or an empty map when
// Redis is unavailable or the manager has not produced a snapshot yet.
func (s *Server) readSlowSnapshot(ctx context.Context) map[string]string {
	if s.redis == nil {
		return map[string]string{}
	}
	snap, err := s.redis.HGetAll(ctx, statsSlowKey).Result()
	if err != nil {
		slog.Warn("stats: read slow snapshot failed", "error", err)
		return map[string]string{}
	}
	return snap
}

// RunSlowStatsLoop refreshes the full-scan figures off the request path
// (manager only, non-blocking): seed on boot, then every statsSlowInterval.
func RunSlowStatsLoop(ctx context.Context, db *sql.DB, rdb *redis.Client) {
	if rdb == nil {
		slog.Warn("stats: slow-figure loop disabled, no redis — serp/enrich completed, urls_found, unique_domains, providers will report unknown")
		return
	}
	refreshSlowStats(ctx, db, rdb)
	ticker := time.NewTicker(statsSlowInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refreshSlowStats(ctx, db, rdb)
		}
	}
}

// refreshSlowStats computes each slow figure independently and stores whichever
// succeed; a failed figure keeps its previous value until the key TTL lapses.
func refreshSlowStats(ctx context.Context, db *sql.DB, rdb *redis.Client) {
	start := time.Now()
	fields := map[string]any{}
	for _, f := range slowFigures {
		var n int64
		if err := statQuery(ctx, db, statsSlowTimeout, f.sql, &n); err != nil {
			slog.Warn("stats: slow figure failed", "figure", f.key, "error", err)
			continue
		}
		fields[f.key] = strconv.FormatInt(n, 10)
	}
	var providers string
	if err := statQuery(ctx, db, statsSlowTimeout, providersSQL, &providers); err != nil {
		slog.Warn("stats: slow figure failed", "figure", providersKey, "error", err)
	} else {
		fields[providersKey] = providers
	}
	if len(fields) == 0 {
		return
	}
	fields[statsSnapshotField] = strconv.FormatInt(time.Now().Unix(), 10)
	pipe := rdb.Pipeline()
	pipe.HSet(ctx, statsSlowKey, fields)
	pipe.Expire(ctx, statsSlowKey, statsSlowKeyTTL)
	if _, err := pipe.Exec(ctx); err != nil {
		slog.Warn("stats: store slow snapshot failed", "error", err)
		return
	}
	slog.Debug("stats: slow snapshot refreshed", "figures", len(fields)-1, "took", time.Since(start))
}

// renamePrefix rewrites "from." to "to." in figure names (v1 calls the results
// block "contacts").
func renamePrefix(names []string, from, to string) []string {
	out := make([]string, len(names))
	for i, n := range names {
		if rest, ok := strings.CutPrefix(n, from+"."); ok {
			n = to + "." + rest
		}
		out[i] = n
	}
	return out
}
