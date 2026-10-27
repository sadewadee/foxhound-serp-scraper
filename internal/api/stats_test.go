package api

import (
	"context"
	"database/sql"
	"slices"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
	"github.com/sadewadee/serp-scraper/internal/testpg"
)

const statsTestSchema = `
CREATE TABLE queries (id serial PRIMARY KEY, status text);
CREATE TABLE serp_jobs (id serial PRIMARY KEY, status text, result_count int DEFAULT 0, updated_at timestamptz DEFAULT now());
CREATE TABLE enrichment_jobs (id serial PRIMARY KEY, status text, completed_at timestamptz DEFAULT now());
CREATE TABLE emails (id serial PRIMARY KEY, domain text, created_at timestamptz DEFAULT now());
CREATE TABLE business_listings (id serial PRIMARY KEY, domain text);
INSERT INTO queries (status) SELECT 'completed' FROM generate_series(1,5);
INSERT INTO queries (status) SELECT 'pending' FROM generate_series(1,2);
INSERT INTO serp_jobs (status, result_count) SELECT 'completed', 10 FROM generate_series(1,6);
INSERT INTO serp_jobs (status) SELECT 'new' FROM generate_series(1,3);
INSERT INTO serp_jobs (status) VALUES ('failed');
INSERT INTO enrichment_jobs (status) SELECT 'completed' FROM generate_series(1,4);
INSERT INTO enrichment_jobs (status) VALUES ('dead'), ('pending');
INSERT INTO emails (domain) SELECT 'gmail.com' FROM generate_series(1,3);
INSERT INTO emails (domain) VALUES ('yahoo.com');
INSERT INTO business_listings (domain) VALUES ('a.com'), ('a.com'), ('b.com');
ANALYZE;
`

func newStatsTestServer(t *testing.T, db *sql.DB) *Server {
	t.Helper()
	mr := miniredis.RunT(t)
	return &Server{db: db, redis: redis.NewClient(&redis.Options{Addr: mr.Addr()})}
}

func TestComputeDashboardStats(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_stats_"}).DB()
	ctx := context.Background()
	if _, err := db.Exec(statsTestSchema); err != nil {
		t.Fatal(err)
	}
	s := newStatsTestServer(t, db)

	// Before the background loop has run, slow figures are unknown (null), not 0.
	st, err := s.computeDashboardStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"serp.completed", "serp.urls_found", "enrich.completed", "results.unique_domains", "results.providers"} {
		if !slices.Contains(st.Unknown, k) {
			t.Errorf("%s should be unknown before the slow snapshot exists; unknown=%v", k, st.Unknown)
		}
	}
	if st.Serp["completed"] != nil {
		t.Errorf("serp.completed = %v, want nil", st.Serp["completed"])
	}
	if st.Serp["pending"] != int64(3) || st.Serp["failed"] != int64(1) {
		t.Errorf("fast serp figures wrong: %v", st.Serp)
	}

	refreshSlowStats(ctx, db, s.redis)
	st, err = s.computeDashboardStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if len(st.Unknown) != 0 {
		t.Fatalf("unexpected unknown figures: %v", st.Unknown)
	}
	checks := []struct {
		name      string
		got, want any
	}{
		{"queries.total", st.Queries["total"], int64(7)},
		{"queries.pending", st.Queries["pending"], int64(2)},
		{"serp.total (reltuples)", st.Serp["total"], int64(10)},
		{"serp.completed", st.Serp["completed"], int64(6)},
		{"serp.urls_found", st.Serp["urls_found"], int64(60)},
		{"serp.today", st.Serp["today"], int64(6)},
		{"enrich.total (reltuples)", st.Enrich["total"], int64(6)},
		{"enrich.completed", st.Enrich["completed"], int64(4)},
		{"enrich.dead", st.Enrich["dead"], int64(1)},
		{"results.total_emails (reltuples)", st.Results["total_emails"], int64(4)},
		{"results.emails_today", st.Results["emails_today"], int64(4)},
		{"results.unique_domains", st.Results["unique_domains"], int64(2)},
	}
	for _, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
	if p, ok := st.Results["providers"].(map[string]int64); !ok || p["gmail.com"] != 3 {
		t.Errorf("providers = %v", st.Results["providers"])
	}
	if !slices.Contains(st.Estimated, "serp.total") {
		t.Errorf("serp.total must be flagged estimated: %v", st.Estimated)
	}
	if st.SnapshotAt == nil {
		t.Error("snapshot_at must be set after the slow loop ran")
	}
}

// One failing statement must not zero or abort the others: the failed figures are
// null + listed as unknown, every other figure keeps its real value.
func TestComputeDashboardStats_OneFailureIsolated(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_stats_"}).DB()
	ctx := context.Background()
	if _, err := db.Exec(statsTestSchema); err != nil {
		t.Fatal(err)
	}
	s := newStatsTestServer(t, db)
	refreshSlowStats(ctx, db, s.redis)

	// Break only emails: its windowed counts error out ("relation does not exist").
	if _, err := db.Exec(`ALTER TABLE emails RENAME TO emails_gone`); err != nil {
		t.Fatal(err)
	}
	st, err := s.computeDashboardStats(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"results.emails_today", "results.emails_per_hour", "results.total_emails"} {
		if !slices.Contains(st.Unknown, k) {
			t.Errorf("%s should be unknown; unknown=%v", k, st.Unknown)
		}
	}
	if st.Results["emails_today"] != nil || st.Results["total_emails"] != nil {
		t.Errorf("failed figures must be nil, got %v", st.Results)
	}
	if st.Serp["pending"] != int64(3) || st.Serp["completed"] != int64(6) || st.Enrich["dead"] != int64(1) {
		t.Errorf("healthy figures were affected by the failure: serp=%v enrich=%v", st.Serp, st.Enrich)
	}
	if st.Queries["total"] != int64(7) || st.Results["unique_domains"] != int64(2) {
		t.Errorf("queries/results healthy figures lost: %v %v", st.Queries, st.Results)
	}
}

// A statement_timeout must hit one figure only (pg_sleep stands in for a slow
// scan); the shared helper reports the error and a following statement works.
func TestStatQuery_TimeoutDoesNotPoisonNext(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_stats_"}).DB()
	ctx := context.Background()

	var n int64
	if err := statQuery(ctx, db, 100*time.Millisecond, `SELECT pg_sleep(2)::text::bigint`, &n); err == nil {
		t.Fatal("expected a statement_timeout error")
	}
	if err := statQuery(ctx, db, statsFastTimeout, `SELECT 42`, &n); err != nil || n != 42 {
		t.Fatalf("next statement must succeed: n=%d err=%v", n, err)
	}
}

// When nothing can be computed the caller gets an error (never cached or served
// as an all-zero payload).
func TestComputeDashboardStats_AllFailed(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_stats_"}).DB()
	s := newStatsTestServer(t, db) // empty DB: no tables, no snapshot
	if _, err := s.computeDashboardStats(context.Background()); err == nil {
		t.Fatal("expected an error when every figure failed")
	}
}
