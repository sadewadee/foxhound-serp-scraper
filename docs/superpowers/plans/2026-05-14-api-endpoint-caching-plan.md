# API Endpoint Caching Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Eliminate timeouts on `/api/v2/{stats,queries,results}` under high-traffic + large-data scenarios by adding Redis-backed caching, statement_timeout protection, and cursor pagination.

**Architecture:** Three layers. (1) Shared `cache.go` helper providing `cachedCount`, `trySingleFlight`, `waitForCache`. (2) Endpoint-specific cache usage — `/api/v2/stats` wraps full response, `/api/v2/queries` caches count + adds statement_timeout, `/api/v2/results` adds cursor pagination as additive query param. (3) Observability via `X-Cache` response header (`HIT|MISS|WAITED|BYPASS`).

**Tech Stack:** Go 1.25, `database/sql`, `github.com/redis/go-redis/v9` (already used), `github.com/alicebob/miniredis/v2` (new test-only dep). No production-binary deps added.

**Spec:** `docs/superpowers/specs/2026-05-14-api-endpoint-caching-design.md`

---

## Test Strategy (read before starting)

**TDD discipline applies to pure-logic code. Redis-coordination tests use `miniredis`. DB-touching code is verified manually via `X-Cache` header + staging traffic.**

| Code type | Test approach |
|---|---|
| `buildCountCacheKey`, `encodeCursor`, `decodeCursor` | Standard unit tests, no deps |
| `trySingleFlight`, `waitForCache` | `miniredis` (test-only dep) |
| `cachedCount` (touches DB) | Manual verification + staging — no `sqlmock` to keep test surface small |
| Handler glue | Manual verification via dashboard polling + `X-Cache` header in browser DevTools |

This trade-off is deliberate: the cache logic that's hard to test (single-flight race, lock TTL boundary) gets `miniredis` coverage. The glue that's easy to verify manually doesn't get a 200-line mock setup. Reference: @superpowers:test-driven-development for the discipline; this plan exercises judgment about test ROI inside that discipline.

---

## File Structure

| File | Action | Responsibility |
|---|---|---|
| `internal/api/cache.go` | **Create** | Shared cache helpers: `cachedCount`, `trySingleFlight`, `waitForCache`, `cachedJSON`. Also re-homes `buildCountCacheKey` from `v2_handlers_results.go`. |
| `internal/api/cache_test.go` | **Create** | Tests for pure functions + miniredis-backed Redis coordination tests. |
| `internal/api/cursor.go` | **Create** | `encodeCursor`/`decodeCursor` for opaque base64 cursor format. |
| `internal/api/cursor_test.go` | **Create** | Round-trip + malformed input tests for cursor codec. |
| `internal/api/v2_handlers_results.go` | **Modify** | (a) Remove `buildCountCacheKey` (moved). (b) Refactor `cachedFilteredCount` to call new `cachedCount`. (c) Add cursor branch to `handleV2ListResults`. |
| `internal/api/v2_handlers_queries.go` | **Modify** | Refactor `handleV2ListQueries` to use `cachedCount` + wrap in tx with `statement_timeout=10s`. |
| `internal/api/v2_handlers_stats.go` | **Modify** | Refactor `handleV2DashboardStats` — extract `computeDashboardStats` from cache-aware wrapper. |
| `docs/api-response.md` | **Modify** | Document cursor query param + response shape for `/api/v2/results`. |
| `go.mod`, `go.sum` | **Modify** | Add `github.com/alicebob/miniredis/v2` as test-only dependency. |

---

## Task 1: Shared cache helpers + miniredis dep

**Files:**
- Create: `internal/api/cache.go`
- Create: `internal/api/cache_test.go`
- Modify: `go.mod`, `go.sum`
- Modify: `internal/api/v2_handlers_results.go:22,30-79` (remove `buildCountCacheKey` + refactor `cachedFilteredCount`)

- [ ] **Step 1: Add miniredis test dependency**

Run:
```bash
cd /Users/sadewadee/Downloads/Plugin\ Pro/serp-scraper
go get -t github.com/alicebob/miniredis/v2@latest
```
Expected: `go: added github.com/alicebob/miniredis/v2 vX.Y.Z`. `go.mod` should now contain the dep.

- [ ] **Step 2: Write failing test for `buildCountCacheKey`**

Create `internal/api/cache_test.go`:

```go
package api

import (
	"testing"
)

func TestBuildCountCacheKey_StableAcrossCalls(t *testing.T) {
	where := "WHERE bl.domain = $1"
	args := []any{"example.com"}
	a := buildCountCacheKey(where, args)
	b := buildCountCacheKey(where, args)
	if a != b {
		t.Fatalf("expected stable key, got %q vs %q", a, b)
	}
	if a == "" {
		t.Fatal("expected non-empty key")
	}
}

func TestBuildCountCacheKey_DifferentArgs(t *testing.T) {
	where := "WHERE bl.domain = $1"
	a := buildCountCacheKey(where, []any{"a.com"})
	b := buildCountCacheKey(where, []any{"b.com"})
	if a == b {
		t.Fatalf("expected different keys for different args")
	}
}

// Regression: %v formatting would collide ["a","bc"] with ["ab","c"].
func TestBuildCountCacheKey_NoTransposeCollision(t *testing.T) {
	a := buildCountCacheKey("WHERE x = $1 AND y = $2", []any{"a", "bc"})
	b := buildCountCacheKey("WHERE x = $1 AND y = $2", []any{"ab", "c"})
	if a == b {
		t.Fatal("transpose collision — %q must separate args")
	}
}
```

Run:
```bash
go test -tags playwright,tls ./internal/api/ -run TestBuildCountCacheKey -v
```
Expected: FAIL with `undefined: buildCountCacheKey` (function lives in `v2_handlers_results.go` still, but package compiles — wait, it's same package). Actually expect: PASS (function exists in same `package api`). Goal of this step is to capture current behavior before move.

If PASS: tests now codify current behavior. If FAIL: investigate before moving.

- [ ] **Step 3: Create `internal/api/cache.go` with `buildCountCacheKey` moved from results handler**

Create `internal/api/cache.go`:

```go
package api

import (
	"context"
	"crypto/sha1"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"
)

// buildCountCacheKey hashes WHERE + args into a stable Redis key.
// %q separates adjacent string args so ["a","bc"] and ["ab","c"] don't collide.
func buildCountCacheKey(where string, args []any) string {
	h := sha1.New()
	h.Write([]byte(where))
	for _, a := range args {
		fmt.Fprintf(h, "|%q|", a)
	}
	return "v2:count:" + hex.EncodeToString(h.Sum(nil))
}

// cachedCount returns COUNT(*) for `SELECT COUNT(*) FROM <fromClause> <where>`.
// Caches result in Redis with TTL. On query timeout or DB error returns (-1, err)
// so callers can serve `count_known=false` instead of HTTP 500.
//
// fromClause includes any join alias, e.g. "business_listings bl".
// where includes the leading "WHERE", e.g. "WHERE bl.domain = $1" or "WHERE 1=1".
func (s *Server) cachedCount(ctx context.Context, fromClause, where string, args []any, ttl, timeout time.Duration) (int, error) {
	key := buildCountCacheKey(fromClause+"|"+where, args)

	if s.redis != nil {
		if v, err := s.redis.Get(ctx, key).Result(); err == nil {
			if n, perr := strconv.Atoi(v); perr == nil {
				return n, nil
			}
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return -1, err
	}
	defer tx.Rollback()

	timeoutMs := int(timeout.Milliseconds())
	if _, err := tx.ExecContext(ctx, fmt.Sprintf("SET LOCAL statement_timeout = '%d'", timeoutMs)); err != nil {
		return -1, err
	}

	var total int
	q := fmt.Sprintf("SELECT COUNT(*) FROM %s %s", fromClause, where)
	if err := tx.QueryRowContext(ctx, q, args...).Scan(&total); err != nil {
		return -1, err
	}
	if err := tx.Commit(); err != nil {
		return -1, err
	}

	if s.redis != nil {
		_ = s.redis.Set(ctx, key, strconv.Itoa(total), ttl).Err()
	}
	return total, nil
}

// trySingleFlight acquires a Redis NX EX lock for the given key.
// Returns true if caller should compute the value, false if another caller
// is already computing. lockTTL bounds the maximum work duration.
func (s *Server) trySingleFlight(ctx context.Context, key string, lockTTL time.Duration) bool {
	if s.redis == nil {
		return true // no Redis = no coordination = every caller computes
	}
	ok, err := s.redis.SetNX(ctx, key+":lock", "1", lockTTL).Result()
	if err != nil {
		slog.Debug("single-flight lock error", "key", key, "error", err)
		return true // Redis errored — degrade to compute-anyway
	}
	return ok
}

// releaseSingleFlight removes the lock. Best-effort; lock TTL is the safety net.
func (s *Server) releaseSingleFlight(ctx context.Context, key string) {
	if s.redis == nil {
		return
	}
	_ = s.redis.Del(ctx, key+":lock").Err()
}

// waitForCache polls a cache key up to maxWait. Returns value+true if populated,
// "" + false on timeout.
func (s *Server) waitForCache(ctx context.Context, key string, maxWait time.Duration) (string, bool) {
	if s.redis == nil {
		return "", false
	}
	deadline := time.Now().Add(maxWait)
	for time.Now().Before(deadline) {
		if v, err := s.redis.Get(ctx, key).Result(); err == nil {
			return v, true
		}
		select {
		case <-ctx.Done():
			return "", false
		case <-time.After(50 * time.Millisecond):
		}
	}
	return "", false
}

// cachedJSON serves a JSON response from Redis cache or computes and caches it.
// Adds X-Cache: HIT|WAITED|MISS|BYPASS header for observability.
//
// On cache miss the caller acquires a single-flight lock. Other callers wait
// up to waitMs for the cache to populate. If they time out, they compute
// anyway (degraded mode — better than failing).
func (s *Server) cachedJSON(
	ctx context.Context,
	w http.ResponseWriter,
	key string,
	ttl time.Duration,
	waitOnMiss time.Duration,
	compute func() (any, error),
) {
	// Try cache.
	if s.redis != nil {
		if cached, err := s.redis.Get(ctx, key).Result(); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "HIT")
			io.WriteString(w, cached)
			return
		}
	}

	// Single-flight.
	gotLock := s.trySingleFlight(ctx, key, 20*time.Second)
	if !gotLock {
		if cached, ok := s.waitForCache(ctx, key, waitOnMiss); ok {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("X-Cache", "WAITED")
			io.WriteString(w, cached)
			return
		}
		// Timed out waiting — fall through to compute (degraded).
	}
	defer s.releaseSingleFlight(context.Background(), key)

	value, err := compute()
	if err != nil {
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to compute response")
		return
	}

	body, err := json.Marshal(value)
	if err != nil {
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to marshal response")
		return
	}

	if s.redis != nil {
		_ = s.redis.Set(ctx, key, body, ttl).Err()
	}
	w.Header().Set("Content-Type", "application/json")
	if gotLock {
		w.Header().Set("X-Cache", "MISS")
	} else {
		w.Header().Set("X-Cache", "BYPASS")
	}
	w.Write(body)
}

// _ keeps sql import used even if a future refactor removes the only call site.
var _ = sql.ErrNoRows
```

- [ ] **Step 4: Remove `buildCountCacheKey` + `resultsCountTTL` + `cachedFilteredCount` body from `v2_handlers_results.go`**

In `/Users/sadewadee/Downloads/Plugin Pro/serp-scraper/internal/api/v2_handlers_results.go`:

Delete lines 20-79 (the `resultsCountTTL` const, `cachedFilteredCount` method, and `buildCountCacheKey` function). Replace with:

```go
// resultsCountTTL is the cache TTL for filtered COUNT(*) on business_listings.
const resultsCountTTL = 60 * time.Second

// cachedFilteredCount is the existing call signature, now thin wrapper over cachedCount.
func (s *Server) cachedFilteredCount(ctx context.Context, where string, args []any) (int, error) {
	return s.cachedCount(ctx, "business_listings bl", where, args, resultsCountTTL, 12*time.Second)
}
```

Note: `buildCountCacheKey` is now in `cache.go` and accessible within the same package.

- [ ] **Step 5: Run cache tests + build**

Run:
```bash
go test -tags playwright,tls ./internal/api/ -run TestBuildCountCacheKey -v
go build -tags playwright,tls ./...
```
Expected: tests PASS, build clean (no output).

- [ ] **Step 6: Write failing tests for `trySingleFlight` and `waitForCache` using miniredis**

Append to `internal/api/cache_test.go`:

```go
import (
	"context"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

func newTestServer(t *testing.T) (*Server, *miniredis.Miniredis) {
	t.Helper()
	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	s := &Server{redis: rdb}
	return s, mr
}

func TestTrySingleFlight_FirstCallerWins(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()

	if !s.trySingleFlight(ctx, "test:key", 5*time.Second) {
		t.Fatal("first caller must win the lock")
	}
	if s.trySingleFlight(ctx, "test:key", 5*time.Second) {
		t.Fatal("second caller must NOT get the lock while held")
	}
}

func TestTrySingleFlight_AfterRelease(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()

	s.trySingleFlight(ctx, "test:key", 5*time.Second)
	s.releaseSingleFlight(ctx, "test:key")
	if !s.trySingleFlight(ctx, "test:key", 5*time.Second) {
		t.Fatal("lock must be acquirable after release")
	}
}

func TestTrySingleFlight_NoRedis(t *testing.T) {
	s := &Server{redis: nil}
	if !s.trySingleFlight(context.Background(), "test:key", 5*time.Second) {
		t.Fatal("nil redis must degrade to compute-anyway (true)")
	}
}

func TestWaitForCache_Populated(t *testing.T) {
	s, mr := newTestServer(t)
	ctx := context.Background()

	mr.Set("test:key", `{"hello":"world"}`)
	v, ok := s.waitForCache(ctx, "test:key", 1*time.Second)
	if !ok {
		t.Fatal("expected cache hit")
	}
	if v != `{"hello":"world"}` {
		t.Fatalf("unexpected value: %q", v)
	}
}

func TestWaitForCache_TimesOut(t *testing.T) {
	s, _ := newTestServer(t)
	ctx := context.Background()

	start := time.Now()
	_, ok := s.waitForCache(ctx, "test:nonexistent", 200*time.Millisecond)
	elapsed := time.Since(start)
	if ok {
		t.Fatal("expected timeout")
	}
	if elapsed < 200*time.Millisecond || elapsed > 400*time.Millisecond {
		t.Fatalf("waitForCache elapsed %v outside expected 200-400ms window", elapsed)
	}
}
```

Note: imports merge into the existing import block — Go formatter handles ordering.

Run:
```bash
go test -tags playwright,tls ./internal/api/ -run TestTrySingleFlight -v
go test -tags playwright,tls ./internal/api/ -run TestWaitForCache -v
```
Expected: tests PASS (implementation already in `cache.go` from Step 3).

- [ ] **Step 7: Commit Task 1**

```bash
git add internal/api/cache.go internal/api/cache_test.go \
        internal/api/v2_handlers_results.go go.mod go.sum
git commit -m "Add shared cache helpers (cachedCount, single-flight, cachedJSON)

- Move buildCountCacheKey + cachedFilteredCount logic into cache.go
- Add trySingleFlight/releaseSingleFlight/waitForCache for thundering-herd
- Add cachedJSON for full-response cache (X-Cache observability header)
- Tests via miniredis (test-only dep)

Existing cachedFilteredCount preserved as thin wrapper for back-compat.
No production binary deps added."
```

---

## Task 2: `/api/v2/queries` — statement_timeout + count cache

**Why P0:** Currently the only one of the 3 endpoints with NO `statement_timeout`. An ILIKE-search on unindexed `text` column on a growing `queries` table will hang the manager's connection pool indefinitely.

**Files:**
- Modify: `internal/api/v2_handlers_queries.go:15-75` (`handleV2ListQueries`)

- [ ] **Step 1: Read current handler shape**

Re-read `internal/api/v2_handlers_queries.go:15-75` to understand the count + data query flow before editing. The bug to preserve: `total int` is captured but `Scan` error is silently dropped — keep that behavior to avoid scope creep (separate ticket for error logging).

- [ ] **Step 2: Refactor `handleV2ListQueries`**

Replace `internal/api/v2_handlers_queries.go` lines 15-75 with:

```go
// handleV2ListQueries returns paginated queries wrapped in V2 format.
// COUNT is cached in Redis (60s TTL) and protected by a 10s statement_timeout.
func (s *Server) handleV2ListQueries(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := v2RequestContext(r)
	defer cancel()

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

	// Cached count with 10s statement_timeout. -1 sentinel on timeout.
	total, err := s.cachedCount(ctx, "queries", where, args, 60*time.Second, 10*time.Second)
	if err != nil {
		slog.Warn("v2: queries count failed (serving with -1 sentinel)", "error", err)
	}

	dataQuery := fmt.Sprintf(`
		SELECT id, text, status, result_count, COALESCE(error_msg,''), created_at
		FROM queries %s
		ORDER BY id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)

	rows, err := s.db.QueryContext(ctx, dataQuery, args...)
	if err != nil {
		slog.Error("v2: list queries error", "error", err)
		writeV2Error(w, http.StatusInternalServerError, "internal_error", "failed to fetch queries")
		return
	}
	defer rows.Close()

	type queryRow struct {
		ID          int64     `json:"id"`
		Text        string    `json:"text"`
		Status      string    `json:"status"`
		ResultCount int       `json:"result_count"`
		Error       string    `json:"error"`
		CreatedAt   time.Time `json:"created_at"`
	}

	queries := []queryRow{}
	for rows.Next() {
		var qr queryRow
		rows.Scan(&qr.ID, &qr.Text, &qr.Status, &qr.ResultCount, &qr.Error, &qr.CreatedAt)
		queries = append(queries, qr)
	}

	writeV2Paginated(w, queries, total, page, perPage)
}
```

Key changes from current code:
- Replace direct `s.db.QueryRow("SELECT COUNT(*) ...")` with `s.cachedCount(...)`.
- `cachedCount` internally wraps in tx + `SET LOCAL statement_timeout`.
- `ctx, cancel := v2RequestContext(r)` added (was missing — other v2 handlers have it).
- Use `QueryContext` instead of `Query` for the data query (uses request context).

- [ ] **Step 3: Build + verify**

Run:
```bash
go build -tags playwright,tls ./...
go test -tags playwright,tls ./internal/api/ -v
```
Expected: build clean, tests PASS (Task 1 tests still pass — no regression).

- [ ] **Step 4: Commit**

```bash
git add internal/api/v2_handlers_queries.go
git commit -m "Protect /api/v2/queries with statement_timeout + count cache

Currently the only v2 list endpoint without statement_timeout — an
ILIKE-search on unindexed queries.text can hang the manager's
connection pool indefinitely as the table grows.

- Wrap COUNT in cachedCount helper (10s timeout, 60s Redis TTL)
- Use QueryContext for data query (request context cancellation)
- On count timeout, serve page with total=-1 sentinel via writeV2Paginated
  (consumers already handle count_known=false in PaginationMeta)"
```

---

## Task 3: `/api/v2/stats` — full-response cache + single-flight

**Files:**
- Modify: `internal/api/v2_handlers_stats.go:9-146`

- [ ] **Step 1: Extract `computeDashboardStats` from current handler**

Replace `internal/api/v2_handlers_stats.go` content with:

```go
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

// computeDashboardStats runs the 5 COUNT(*) queries against the live DB.
// Wrapped in cachedJSON above. Statement timeout 15s.
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
	qd, _ := s.redis.ZCard(ctx, "serp:queue:queries").Result()
	queueMap["serp:queue:queries"] = qd
	sb, _ := s.redis.LLen(ctx, "serp:buffer").Result()
	queueMap["serp:buffer"] = sb
	eb, _ := s.redis.LLen(ctx, "enrich:buffer").Result()
	queueMap["enrich:buffer"] = eb

	// Wrap in V2 envelope manually since we're not going through writeV2Single.
	// cachedJSON caches the entire envelope so subsequent hits return identical
	// shape without re-wrapping.
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
```

Notable changes from original:
- Body of original `handleV2DashboardStats` becomes `computeDashboardStats`.
- New `handleV2DashboardStats` is a thin wrapper that delegates to `cachedJSON`.
- The V2 envelope `{"data": ...}` is constructed inline because `cachedJSON` caches raw JSON bytes (not the Go map). Going through `writeV2Single` inside the cached path would re-wrap on every cache miss.

- [ ] **Step 2: Build + run all api tests**

Run:
```bash
go build -tags playwright,tls ./...
go test -tags playwright,tls ./internal/api/ -v
```
Expected: build clean, Task 1 tests still PASS.

- [ ] **Step 3: Manual smoke (skip if no local PG/Redis)**

If you have local PG+Redis (e.g. via `docker compose up db redis`), spin manager:
```bash
go run -tags playwright,tls . run -stage none
```
Then in another terminal:
```bash
curl -i -H "x-api-key: $API_KEY" http://localhost:8080/api/v2/stats | head -10
curl -i -H "x-api-key: $API_KEY" http://localhost:8080/api/v2/stats | head -10
```
Expected: first response `X-Cache: MISS`, second response `X-Cache: HIT` within 30s.

If no local env: skip — verified post-deploy.

- [ ] **Step 4: Commit**

```bash
git add internal/api/v2_handlers_stats.go
git commit -m "Cache /api/v2/stats response with 30s TTL + single-flight

Dashboard polls every 5-10s with 3-5 concurrent users. Without cache,
each request runs 5 sequential COUNT(*) on serp_jobs (5.7M) +
enrichment_jobs + emails (828K) + business_listings + emails group-by.
At scale that's 30-60 stat req/min × ~15s each = pool exhaustion.

- Wrap response in cachedJSON helper (30s TTL)
- Single-flight via Redis NX EX prevents thundering herd at TTL expiry
- Extract compute path as computeDashboardStats for testability
- Response shape unchanged (cached body is the full V2 envelope)
- X-Cache: HIT|WAITED|MISS|BYPASS for observability"
```

---

## Task 4: `/api/v2/results` — cursor pagination

**Files:**
- Create: `internal/api/cursor.go`
- Create: `internal/api/cursor_test.go`
- Modify: `internal/api/v2_handlers_results.go` (add cursor branch to `handleV2ListResults`)
- Modify: `docs/api-response.md`

- [ ] **Step 1: Write failing tests for cursor codec**

Create `internal/api/cursor_test.go`:

```go
package api

import (
	"testing"
)

func TestCursor_RoundTrip(t *testing.T) {
	encoded := encodeCursor(12345)
	if encoded == "" {
		t.Fatal("expected non-empty cursor")
	}
	id, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if id != 12345 {
		t.Fatalf("expected id=12345, got %d", id)
	}
}

func TestCursor_EmptyDecode(t *testing.T) {
	_, err := decodeCursor("")
	if err == nil {
		t.Fatal("empty cursor must error")
	}
}

func TestCursor_MalformedDecode(t *testing.T) {
	_, err := decodeCursor("not-base64!!!")
	if err == nil {
		t.Fatal("malformed cursor must error")
	}
}

func TestCursor_MalformedJSON(t *testing.T) {
	// Valid base64 but not JSON
	_, err := decodeCursor("aGVsbG8=") // "hello"
	if err == nil {
		t.Fatal("non-JSON cursor body must error")
	}
}

func TestCursor_NegativeID(t *testing.T) {
	encoded := encodeCursor(-1)
	id, err := decodeCursor(encoded)
	if err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if id != -1 {
		t.Fatalf("expected -1, got %d", id)
	}
}
```

Run:
```bash
go test -tags playwright,tls ./internal/api/ -run TestCursor -v
```
Expected: FAIL with `undefined: encodeCursor` / `undefined: decodeCursor`.

- [ ] **Step 2: Implement cursor codec**

Create `internal/api/cursor.go`:

```go
package api

import (
	"encoding/base64"
	"encoding/json"
	"errors"
)

// cursorPayload is the wire format inside an opaque cursor.
// Versioned via the struct shape so future fields stay backward-compatible.
type cursorPayload struct {
	ID int64 `json:"id"`
}

// encodeCursor builds an opaque base64url cursor from a row ID.
// Used for pagination "next page" pointer.
func encodeCursor(id int64) string {
	payload, _ := json.Marshal(cursorPayload{ID: id})
	return base64.RawURLEncoding.EncodeToString(payload)
}

// decodeCursor unwraps a cursor back to its row ID.
// Returns error for empty, non-base64, or non-JSON inputs.
func decodeCursor(cursor string) (int64, error) {
	if cursor == "" {
		return 0, errors.New("cursor is empty")
	}
	raw, err := base64.RawURLEncoding.DecodeString(cursor)
	if err != nil {
		return 0, err
	}
	var p cursorPayload
	if err := json.Unmarshal(raw, &p); err != nil {
		return 0, err
	}
	return p.ID, nil
}
```

Run:
```bash
go test -tags playwright,tls ./internal/api/ -run TestCursor -v
```
Expected: all PASS.

- [ ] **Step 3: Add cursor branch to `handleV2ListResults`**

Modify `internal/api/v2_handlers_results.go` `handleV2ListResults` (lines 122-261 in original). The change is in the WHERE-clause assembly + pagination logic; the email-fetch section (lines 203-254) is unchanged.

Locate this block (around line 134):

```go
	where, args, argIdx := buildResultsFilter(q)

	// Count total via Redis-cached helper. On timeout/error the helper returns -1
	// so we serve the page with a sentinel instead of 500.
	total, err := s.cachedFilteredCount(ctx, where, args)
	if err != nil {
		slog.Warn("v2: count failed (returning -1 sentinel)", "error", err)
	}
```

Replace with:

```go
	where, args, argIdx := buildResultsFilter(q)

	// Cursor mode: ?cursor=<base64> uses keyset pagination — O(log N) at deep pages.
	// OFFSET mode (existing): ?page=N + ?per_page=M — capped at perPage=200.
	cursorParam := q.Get("cursor")
	useCursor := cursorParam != ""

	var cursorID int64
	if useCursor {
		var err error
		cursorID, err = decodeCursor(cursorParam)
		if err != nil {
			writeV2Error(w, http.StatusBadRequest, "invalid_cursor", "cursor is malformed")
			return
		}
		dir := q.Get("cursor_dir")
		if dir == "prev" {
			where += fmt.Sprintf(" AND bl.id > $%d", argIdx)
		} else {
			where += fmt.Sprintf(" AND bl.id < $%d", argIdx)
		}
		args = append(args, cursorID)
		argIdx++
	}

	// Count is only computed for offset mode. Cursor mode returns has_more flag.
	var total int
	if !useCursor {
		var err error
		total, err = s.cachedFilteredCount(ctx, where, args)
		if err != nil {
			slog.Warn("v2: count failed (returning -1 sentinel)", "error", err)
		}
	}
```

Then locate the existing query construction (around line 144):

```go
	// Query 1: Fetch paginated listings (all columns).
	dataQuery := fmt.Sprintf(`
		SELECT bl.id, COALESCE(bl.business_name,''), ...
		FROM business_listings bl %s
		ORDER BY bl.id DESC
		LIMIT $%d OFFSET $%d
	`, where, argIdx, argIdx+1)
	args = append(args, perPage, offset)
```

Replace the `dataQuery` block + `args = append(args, perPage, offset)` with:

```go
	// Cursor mode: fetch perPage+1 to detect has_more without separate query.
	// OFFSET mode: existing pagination.
	limit := perPage
	if useCursor {
		limit = perPage + 1 // +1 sentinel for has_more detection
	}

	dataQuery := fmt.Sprintf(`
		SELECT bl.id, COALESCE(bl.business_name,''), COALESCE(bl.category,''),
		       COALESCE(bl.description,''), COALESCE(bl.website,''),
		       bl.domain, bl.url, COALESCE(bl.social_links,'{}'),
		       COALESCE(bl.address,''), COALESCE(bl.location,''),
		       COALESCE(bl.city,''), COALESCE(bl.country,''), COALESCE(bl.contact_name,''),
		       COALESCE(bl.opening_hours,''), COALESCE(bl.rating,''),
		       COALESCE(bl.page_title,''), COALESCE(bl.phone,''), COALESCE(bl.phones,'{}'),
		       COALESCE(bl.tiktok,''), COALESCE(bl.youtube,''), COALESCE(bl.telegram,''),
		       bl.source_query_id, bl.created_at, bl.updated_at
		FROM business_listings bl %s
		ORDER BY bl.id DESC
		LIMIT $%d`, where, argIdx)
	args = append(args, limit)
	argIdx++

	if !useCursor {
		dataQuery += fmt.Sprintf(" OFFSET $%d", argIdx)
		args = append(args, offset)
	}
```

Then at the END of the handler (currently `writeV2Paginated(w, listings, total, page, perPage)` around line 260), replace with:

```go
	if useCursor {
		hasMore := len(listings) > perPage
		if hasMore {
			listings = listings[:perPage] // drop sentinel row
		}
		var nextCursor string
		if hasMore && len(listings) > 0 {
			nextCursor = encodeCursor(listings[len(listings)-1].ID)
		}
		writeV2Single(w, map[string]any{
			"data":        listings,
			"next_cursor": nextCursor,
			"has_more":    hasMore,
		})
		return
	}

	writeV2Paginated(w, listings, total, page, perPage)
```

- [ ] **Step 4: Build + run all api tests**

Run:
```bash
go build -tags playwright,tls ./...
go test -tags playwright,tls ./internal/api/ -v
```
Expected: build clean, all tests PASS.

- [ ] **Step 5: Update docs/api-response.md**

Append to `docs/api-response.md` (after the existing `/api/v2/results` section):

```markdown
### Cursor pagination for `/api/v2/results` (v0.8.0+)

Two pagination modes are supported on the same endpoint. Choose based on UX:

| Mode | Use when | Trigger param |
|---|---|---|
| OFFSET (default) | Need total count, jumping to arbitrary page (page 5, 10, etc.) | `?page=N&per_page=M` |
| Cursor | "Load more" UX, deep pagination, no total count needed | `?cursor=<base64>&per_page=M` |

Cursor request:
```
GET /api/v2/results?cursor=eyJpZCI6MTIzNDV9&per_page=50
```

Cursor response (200):
```json
{
  "data": [ /* business listings */ ],
  "next_cursor": "eyJpZCI6MTIyOTV9",
  "has_more": true
}
```

Notes:
- `next_cursor` is an opaque base64-encoded value — treat as a black box.
- When `has_more=false`, `next_cursor` is empty — pagination is done.
- Optional `?cursor_dir=prev` reverses the direction (rare; for back-navigation).
- Cursor mode does NOT return `meta.total` — use `/api/v2/results/count` separately if needed.
```

- [ ] **Step 6: Commit**

```bash
git add internal/api/cursor.go internal/api/cursor_test.go \
        internal/api/v2_handlers_results.go docs/api-response.md
git commit -m "Add cursor pagination to /api/v2/results (additive)

OFFSET pagination degrades at deep pages — OFFSET 50,000 scans 50K rows
just to skip them. For 'load more' UX at 700K+ business_listings scale,
keyset (cursor) pagination is O(log N) via the existing PK index.

- Add encodeCursor/decodeCursor (base64url + JSON payload, versioned shape)
- handleV2ListResults branches on ?cursor=...:
    OFFSET mode (existing): unchanged, ?page + ?per_page
    cursor mode (new):      ?cursor + ?per_page, returns next_cursor + has_more
- Fetch perPage+1 to detect has_more in single query
- Optional ?cursor_dir=prev for back-navigation
- Documented in docs/api-response.md

No breaking change. Frontend opts in by passing cursor instead of page."
```

---

## Task 5: Integration verification

**Files:** none modified — pure verification.

- [ ] **Step 1: Full build (production target)**

Run:
```bash
CGO_ENABLED=0 go build -tags playwright,tls -ldflags="-w -s" -o /tmp/serp-scraper-build .
ls -lh /tmp/serp-scraper-build
rm /tmp/serp-scraper-build
```
Expected: binary ~30-35MB, no compile error.

- [ ] **Step 2: Full test suite**

Run:
```bash
go test -tags playwright,tls ./... 2>&1 | tail -30
```
Expected: all packages PASS, no failures. Verify especially:
- `ok  github.com/sadewadee/serp-scraper/internal/api` with N+ tests.
- No regression in `internal/stage/...`, `internal/scraper/...`.

- [ ] **Step 3: Race detector on api package**

Run:
```bash
go test -tags playwright,tls -race -count=1 ./internal/api/...
```
Expected: ok, no race conditions. Single-flight + waitForCache use goroutine coordination — race detector validates we got it right.

- [ ] **Step 4: Vet**

Run:
```bash
go vet -tags playwright,tls ./...
```
Expected: no output (clean).

- [ ] **Step 5: Final summary (no commit)**

Run:
```bash
git log --oneline -5
git diff --stat origin/canary/v0.8.0-country-extraction..HEAD
```
Expected: 4 commits ahead of remote (one per Task 1-4), `~400-500` lines net delta.

---

## Rollout

After all tasks land:

1. Push branch: `git push origin canary/v0.8.0-country-extraction`
2. Build canary image on kurama via `docker-compose.build.yaml` (existing infra).
3. Push to GHCR `ghcr.io/sadewadee/foxhound-serp-scraper:canary-latest`.
4. Redeploy via Dokploy.
5. Verify post-deploy:
   - `curl -i -H "x-api-key: $API_KEY" https://manager.../api/v2/stats` — confirm `X-Cache` header present.
   - Repeat curl within 30s — `X-Cache: HIT`.
   - Watch dashboard p95 latency for 1h via Prometheus on `:9090`.
6. Rollback path: revert the 4 commits, redeploy previous tag.

## Success criteria (post-deploy)

- `/api/v2/stats` p95 latency: <100ms (was 5-15s).
- `/api/v2/queries`: zero HTTP 500 from statement timeout under ILIKE search.
- `/api/v2/results?cursor=...`: <500ms at any logical page depth.
- Worker email throughput unchanged (sanity check: no write-path regression).
