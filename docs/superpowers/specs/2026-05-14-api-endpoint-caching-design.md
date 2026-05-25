# API Endpoint Caching — Design

**Date**: 2026-05-14
**Branch**: `canary/v0.8.0-country-extraction`
**Problem**: User-facing v2 endpoints time out at scale. `/api/v2/stats` runs 5 sequential COUNT(*) on multi-million-row tables per request, dashboard polls every 5-10s with 3-5 concurrent users. `/api/v2/queries` has no `statement_timeout` and can hang the connection pool indefinitely. `/api/v2/results` is partially cached but OFFSET pagination degrades at deep pages.

**Non-goals**: counter table + trigger (write amplification), materialized view (staleness), read replica (ops overhead), trigram index for ILIKE (separate concern).

## Scope

Four targeted changes inside `internal/api/v2_handlers_*.go`. No schema migration. No worker changes. No API breaking change.

| # | Endpoint | Change |
|---|---|---|
| 1 | `/api/v2/stats` | Redis cache the full response body, TTL 30s, single key |
| 2 | `/api/v2/queries` (list) | Add `SET LOCAL statement_timeout = '10000'` + Redis-cache the COUNT (TTL 60s, key by filter hash) |
| 3 | `/api/v2/results` (list) | Add cursor pagination as additive query param; existing OFFSET path unchanged |
| 4 | (shared) | Single-flight lock around cache misses to prevent thundering herd at TTL expiry |

## Design — Component 1: `/api/v2/stats` full-response cache

**Cache key**: `v2:stats:dashboard` (single key — endpoint has no filters)

**TTL**: 30s

**Flow**:
```
handleV2DashboardStats:
  if GET v2:stats:dashboard hits → return raw JSON
  else:
    acquire single-flight lock (key: v2:stats:dashboard:lock, NX EX 20s)
    if lock acquired:
      run the 5 COUNT queries inside existing tx (statement_timeout=15s)
      build response JSON
      SET v2:stats:dashboard {json} EX 30
      DEL v2:stats:dashboard:lock
      return JSON
    else:
      wait up to 5s polling cache (50ms intervals)
      if cache populated → return it
      else → return cached-stale-or-503
```

**Failure mode**: Redis down → fall through to current code path (no cache, direct DB query). Single-flight lock degrades to "every request hits DB" — same as today, no regression.

**Stale-while-revalidate**: NOT in v1. Add later if cache-miss latency proves painful.

## Design — Component 2: `/api/v2/queries` count cache + timeout

**Cache key**: `v2:queries:count:<sha1(where|args)>` — same pattern as existing `buildCountCacheKey` in `v2_handlers_results.go:72`.

**TTL**: 60s

**Statement timeout**: 10s (wrapped in `tx` like results.go does for COUNT).

**Flow**: identical to `cachedFilteredCount` in `v2_handlers_results.go:30` — extract into a shared helper `cachedCount(ctx, table, where, args, ttl)` that both endpoints use. Sentinel `-1` on timeout, caller serves page with `count_known=false`.

**Why 60s vs 30s for stats**: queries list count changes less frequently (user manually creates queries, not auto-incrementing); stats counters tick with every job completion (~hundreds/sec at peak).

## Design — Component 3: `/api/v2/results` cursor pagination

**New query params** (additive, optional):
- `cursor` — opaque base64-encoded `last_id` from previous page
- `cursor_dir` — `next` (default) or `prev`

**Cursor format**: `base64url(json{"id": 12345, "ts": "2026-05-14T..."})` — version stays inside JSON for forward-compat.

**Flow**:
```
if ?cursor is present:
  decode → last_id
  WHERE bl.id < $last_id (next)  or  bl.id > $last_id (prev)
  ORDER BY bl.id DESC LIMIT $perPage
  → response includes next_cursor = base64(last_row.id), no total count
elif ?page is present:
  (existing OFFSET path unchanged)
```

**Response shape** (cursor mode): `{data: [...], next_cursor: "...", has_more: true}` — no `total` field. Documented in `docs/api-response.md` update.

**Backward compat**: `?page` continues to work. Frontend can opt-in to cursor for "load more" UX. No client breaking change.

## Design — Component 4: single-flight lock helper

**Pattern**:
```go
// trySingleFlight attempts to acquire a Redis NX EX lock. Returns true if
// caller should compute the value, false if another caller is already computing.
func (s *Server) trySingleFlight(ctx context.Context, key string, ttl time.Duration) (bool, error) {
    ok, err := s.redis.SetNX(ctx, key+":lock", "1", ttl).Result()
    return ok, err
}

// waitForCache polls a cache key up to maxWait, returning value when present.
func (s *Server) waitForCache(ctx context.Context, key string, maxWait time.Duration) (string, bool) {
    deadline := time.Now().Add(maxWait)
    for time.Now().Before(deadline) {
        if v, err := s.redis.Get(ctx, key).Result(); err == nil {
            return v, true
        }
        time.Sleep(50 * time.Millisecond)
    }
    return "", false
}
```

**Where used**: Component 1 only (stats endpoint). Component 2 reuses existing `cachedFilteredCount` semantics (cache miss = each caller computes; cheap enough since query is already `LIMIT`-bounded). Avoiding single-flight on Component 2 keeps surface small.

## Failure modes & fallbacks

| Failure | Behavior |
|---|---|
| Redis down (GET fails) | Fall through to DB path. Same as current. |
| Redis down (SET fails) | Best-effort, log + serve response. |
| Single-flight lock held but holder dies | Lock TTL (20s) frees it. Next caller retries. |
| DB statement_timeout fires | Return `count_known=false` (count endpoint) or 500 (stats/queries) — current behavior. |
| Stale cache served during DB outage | Acceptable for dashboard. Document. |

## Testing

| Test | What | Where |
|---|---|---|
| `TestV2StatsCache_HitMiss` | First call DB, second call cache, after TTL expire DB again | `internal/api/v2_handlers_stats_test.go` (new) |
| `TestV2StatsCache_SingleFlight` | 10 concurrent requests at cache miss → exactly 1 DB call | same |
| `TestV2QueriesCount_CacheKey` | Different filter combos produce different cache keys | `internal/api/v2_handlers_queries_test.go` (new) |
| `TestV2QueriesCount_StatementTimeout` | Slow mock DB → returns sentinel within 10s + 500 buffer | same |
| `TestV2ResultsCursor_Encode` | Cursor round-trip (encode → decode → query) | `internal/api/v2_handlers_results_test.go` (existing file or new) |
| `TestV2ResultsCursor_NextPrev` | next/prev directions return non-overlapping result sets | same |

Redis mock: use `miniredis` or `redis-mock` if already in `go.sum`; otherwise small interface around `redis.Cmdable` mocked manually.

## Out of scope (deferred to separate work items)

- Trigram/full-text index for ILIKE search (affects both `queries.text` and `business_listings.business_name`) — separate ticket
- Read-replica routing for analytics queries — too much ops surface change
- Counter-table + trigger for real-time stats — write amplification risk
- Stale-while-revalidate cache pattern — add only if cache-miss latency complaints arise

## Rollout

- Behind no feature flag — straightforward additive change, low risk.
- Deploy via canary stack (`docker-compose.build.yaml` → GHCR → Dokploy).
- Validate via `docker logs manager 2>&1 | grep "v2:.*cache"` for hit/miss counter — add `slog.Debug` lines.
- Rollback path: revert one commit, redeploy previous tag.

## Success criteria

- `/api/v2/stats` p95 latency drops from current ~5-15s to <100ms under 5 concurrent users.
- `/api/v2/queries` no longer returns 500 when ILIKE search hits unindexed scan.
- `/api/v2/results` page=500 (deep) returns in <500ms via cursor path; existing OFFSET path unchanged for back-compat.
- Zero impact on worker write throughput (no schema changes, no triggers).

## Effort

~150-200 lines net across 3 handler files + 1 new shared cache helper + 6 tests. No migration. Single commit feasible.
