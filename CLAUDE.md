# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

DONT EVENT TRY TO MOVE ANY IMAGE/FILE WITHOUT COMPRESSING AND DONT MOVE VIA TRAEFIK! REMEMBER THIS!

## Overview

Distributed Go 1.25 pipeline that scrapes SERP engines (Bing/DDG/Google) for target domains, then enriches those domains by visiting contact pages and extracting business contact data (emails, phones, social, address). Uses [foxhound](https://github.com/sadewadee/foxhound) (`v0.0.22`) as the scraping engine — Camoufox for browser-required SERPs, stealth HTTP for everything else. Postgres holds durable state + job queues, Redis holds the hot path (dedup sets + transient coordination), and worker containers are horizontally replicated.

The `README.md` describes a 4-stage architecture (`serp → website → contact`). **That is out of date.** The current pipeline has **two scraping stages plus one autonomous re-enricher**: `serp`, `enrich`, `reenrich`. There is no separate `website` stage — contact page discovery is merged into `enrich`. Trust this file over README for stage names.

## Build / Run

Almost every Go file is gated with `//go:build playwright` — you **must** build with the `playwright` tag or you will get "no Go files" errors:

```bash
# Local dev build (single binary)
go build -tags playwright -o serp-scraper .

# Production build (matches Dockerfile flags)
CGO_ENABLED=0 go build -tags playwright,tls -ldflags="-w -s" -o serp-scraper .

# Standalone helper binaries (NOT covered by the playwright tag; build separately)
go build -o backfill-address-fields ./cmd/backfill-address-fields
go build -o dump ./cmd/dump
go build -o proxyforward ./cmd/proxyforward
```

Run modes (all need Postgres + Redis except `scrape`):

| Command | What it does |
|---|---|
| `serp-scraper run -stage all` | SERP discovery + enrich, both stages active |
| `serp-scraper run -stage serp -workers 4` | SERP discovery only (Bing default, tabs per worker) |
| `serp-scraper run -stage enrich -workers 20` | Contact extraction only — workers pop from `enrichment_jobs` |
| `serp-scraper run -stage reenrich -workers 1` | Autonomous re-enrich loop for low-completeness `business_listings` |
| `serp-scraper run -stage none` | **Manager mode** — boots API + Telegram bot only, no scraping. This is what `services.manager` runs in compose. |
| `serp-scraper scrape -q "<query>" -o out.csv` | Standalone, no PG/Redis — quick one-off |
| `serp-scraper import -file queries.csv` / `generate -country "all" -niche fitness` | Seed the `queries` table |
| `serp-scraper status` / `export -output contacts.csv` | Operational |

Generate's `-country` / `-niche` flags drive the wellness keyword generator (`cmd.RunGenerateWellness`); the legacy YAML template path via `-templates` still exists but isn't the primary flow.

## Tests

Run all package tests (no `playwright` tag needed for most — only files that link foxhound require it):

```bash
go test ./...
go test ./internal/stage/... -run TestRecoverCountry -v   # single test
```

Notable test coverage to be aware of when editing:
- `internal/db/migrate_country_test.go` — guards the regex bug that polluted 8,177 country rows in May 2026 (see Operational Invariants below)
- `internal/stage/enrich_test.go`, `internal/stage/reenrich_test.go`, `internal/stage/encoding_test.go` — stage behavior
- `internal/scraper/contact_test.go`, `internal/directory/jsonld_test.go` — extractor regressions
- `internal/monitor/{ticker,rates}_test.go` — metrics math
- `internal/dedup/dedup_test.go` — Redis SET semantics

End-to-end smoke (write-path → read-path):
```bash
POSTGRES_DSN=... API_BASE_URL=http://localhost:8080 API_KEY=... \
  ./scripts/verify-data-pipeline.sh
```
Exit `1` = write-path schema gap, `2` = column populated but missing from `/api/v2/results` (the "fix 80% lupa loop terakhir" pattern that bit us 2026-04-27). Run before any deploy that touches schema, triggers, extractor, or v2 read handlers.

## Deployment / Compose Files

Four compose files, each with a distinct purpose — don't confuse them:

| File | Purpose |
|---|---|
| `docker-compose.yaml` | Primary production stack — builds locally, runs db + redis + manager + serp + enrich + reenrich + autoheal |
| `docker-compose.build.yaml` | **Image build only** — runs on `kurama`, produces `ghcr.io/sadewadee/foxhound-serp-scraper:canary-…` and pushes to GHCR. Requires `GHCR_PAT`. |
| `docker-compose.registry.yaml` | GHCR-image-based deploy (no local build — pulls from `ghcr.io`) |
| `docker-compose.worker.yaml` | Remote-only worker pool (connects back to central Postgres + Redis over Tailscale/private network) |

Production deployment is via **Dokploy on `kurama`/`kurawa`** — edit compose/env locally, commit, push, redeploy via the Dokploy UI. **Never** edit compose files on the server. Server paths are `/home/sadewa/serp-*` only.

## Architecture

### Data flow (multi-file)

```
queries  ──pop──▶ serp_jobs  ──worker.serp──▶ serp_results  ─trigger─▶ enrichment_jobs  ──worker.enrich──▶ business_listings + emails
                                                                                                            ▲
                                                                                              reenrich worker (autonomous)
```

1. **Seed**: `cmd/{import,generate}.go` insert into `queries` (deduped by `text_hash` = `SHA256(lower(trim(text)))`).
2. **SERP fan-out**: `internal/feeder` expands each `queries` row into multiple `serp_jobs` (one per page × engine). `internal/stage/serp.go` workers `SELECT … FOR UPDATE SKIP LOCKED` to claim jobs, fetch via a pooled stealth fetcher (one per `tabWorker`, recycled every `STEALTH_RECYCLE_AFTER` requests — do **not** create+close per request), parse via the `SearchEngine` interface in `internal/scraper/engine.go` (implementations: `bing.go`, `duckduckgo.go`, `google.go`). Results land in `serp_results`.
3. **Enrich**: a Postgres trigger turns `serp_results` rows into `enrichment_jobs`. `internal/stage/enrich.go` workers pop jobs, visit the URL (and `/contact`, `/about` if `contact_pages=true`), and write back to the `raw_*` columns on `enrichment_jobs`. A second trigger fires on `status='completed'` to upsert `business_listings` from those `raw_*` fields.
4. **Re-enrich**: `internal/stage/reenrich.go` is a continuous loop (no Redis queue, no REST trigger) targeting `business_listings` with `re_enriched_at IS NULL` and a low completeness score. Manual trigger: `UPDATE business_listings SET re_enriched_at = NULL WHERE domain IN (...)`.
5. **Persist**: `internal/persist` buffers Redis-side hot data and flushes to Postgres every `PERSIST_INTERVAL_MS` in batches of `PERSIST_BATCH_SIZE`.
6. **Reconcile**: `internal/reconciler` heals stuck `processing` jobs every `RECONCILER_INTERVAL_MS`. **Critical**: it must *increment* `attempt_count`, never reset to 0, and cap at the column's `max_attempts` — see Operational Invariants.

### Process layout (compose)

- `manager` — runs `serp-scraper run -stage none` → spins up `internal/api` (HTTP REST, `:8080`) + `internal/telegram` bot + `internal/monitor` Prometheus exporter (`:9090`). No scraping. Workers don't run the API.
- `serp` worker (replicated by `SERP_WORKER_COUNT`, default 2) — `serp-scraper run -stage serp -workers $SERP_CONCURRENCY`. `BETA_FEATURES=0` is hard-coded in compose because foxhound's circuit breaker is too aggressive for SERP traffic.
- `enrich` worker (replicated by `ENRICH_WORKER_COUNT`, default 4) — `BETA_FEATURES` opt-in via env.
- `reenrich` (replicated by `REENRICH_WORKER_COUNT`, default 1).
- `autoheal` — `willfarrell/autoheal` restarts containers whose healthcheck fails. Healthcheck for workers is `test -f /tmp/worker-healthy` with a 120s freshness window — workers must touch the file periodically.

DB pool is intentionally minimal (`PG_MAX_OPEN_CONNS=2`, `PG_MAX_IDLE_CONNS=1`) — the hot path is zero-DB (Redis only); only `persist` and `reconciler` need connections.

### REST API

`internal/api/server.go` registers two parallel surfaces:

- **v1** (`/api/auth/*`, `/api/health`, `/api/contacts`, `/api/queries`, `/api/pipeline/*`) — bare-shape responses `{...}` / `{"error":"..."}`.
- **v2** (`/api/v2/*`) — envelope responses `{"data": {...}}` / `{"error":{"code":"…","message":"…"}}`. Two roles: `admin` (full mutations) and `viewer` (GET-only). Auth via `Authorization: Bearer <token>` or `x-api-key: <key>`. See `docs/api-response.md` for the full viewer-accessible response inventory.

When adding a new field to `enrichment_jobs` → `business_listings`: schema + trigger + extractor + **v2 read handler in `internal/api/v2_handlers_results.go`** must all be updated together. The smoke script (`scripts/verify-data-pipeline.sh`) is the contract test.

### Directory extractors

`internal/directory/` holds per-source business-listing parsers (ClassPass, Yelp, YellowPages, YogaAlliance, TripAdvisor, plus a generic JSON-LD fallback). Registry lives in `directory.go:init()`. The enrich worker dispatches to whichever extractor's `Match(domain)` returns true; otherwise falls back to the generic contact-page scraper in `internal/scraper/contact.go`.

## Operational Invariants

These are the rules the codebase enforces and that you should never violate. All are recovered from real production incidents (see `.dev-squad/gotchas.md` for the full incident log).

1. **Retry counters never reset to 0.** Reconcilers that heal stuck jobs must *increment* `attempt_count`, cap at `max_attempts` (15 for enrich, 10 for serp), and mark `dead` above the cap. Grep for `attempt_count = 0` after touching any reconciler — the same antipattern keeps creeping back across `serp.go` and `enrich.go`.
2. **No unbounded `COUNT(*)` on big tables.** Every periodic metric / backpressure query must be bounded by a time window + `LIMIT`, and wrapped in `SET LOCAL statement_timeout = '5000'`. `serp_jobs` and `enrichment_jobs` routinely exceed 1M rows.
3. **Backpressure re-queue must score into the future.** When pushing a query back to a Redis sorted set under backpressure, score = `time.Now().Unix() + N`, never the query ID — low IDs land at the front of `ZPOPMIN` and create a tight loop.
4. **Auto-generation has a depth cap.** Query expansion (`expandCompletedQueries`) must set `expanded_at = NOW()` on inserted variants immediately, capping expansion at generation ≤ 1. Without it: 1 → 9 → 81 → 729.
5. **Pool HTTP fetchers — never per-request.** The pattern in `enrich.go` is canonical: one stealth fetcher per worker, recycled every `STEALTH_RECYCLE_AFTER` requests. Creating+closing per request burns TLS handshakes + identity generation.
6. **Country codes need an ISO-2 whitelist post-regex.** Any SQL/Go path that extracts a country code must validate the captured string against `iso3166Alpha2` (or its SQL CASE equivalent) before writing. The `[A-Z]{2,3}` regex polluted 8,177 rows in May 2026 by capturing "AB" from "LS7 1AB", "COM" from "Telecom", etc. — regex match alone is insufficient.
7. **Fail-open paths must log.** Components like Mordibouncer that silently disable when env vars are empty must emit `slog.Warn()`. Operators need to know they're running degraded.
8. **Time-windowed queries need indexes.** Reconciler hits `created_at`/`updated_at`/`picked_at` every 60s — corresponding partial indexes already exist (`idx_serp_feed`, `idx_enrich_stale`, `idx_enrich_completed_at`, etc.). When adding a new periodic query, add the matching index in `migrate.go` in the same commit.
9. **Postgres needs `idle_in_transaction_session_timeout=60000`** in the compose `command:` block. Docker-killed containers leave open transactions otherwise.

## Conventions Specific to This Repo

- **Module path**: `github.com/sadewadee/serp-scraper`. Subpackage imports use this prefix (never relative).
- **Build tag**: Files that touch `playwright-community/playwright-go` or `foxhound/fetch` (browser + stealth) need `//go:build playwright` at the top. Pure-logic files (config, db, dedup, directory parsers, monitor) don't.
- **Sub-commands under `cmd/`**: top-level `cmd/*.go` are dispatched from `main.go` (`RunImport`, `RunGenerate`, `RunPipeline`, etc.). `cmd/backfill-address-fields/`, `cmd/dump/`, `cmd/proxyforward/` are *independent* `main` packages — each is its own binary, built separately.
- **Migrations**: There is no migration framework. `internal/db/migrate.go` ships one `CREATE TABLE IF NOT EXISTS` + `CREATE INDEX IF NOT EXISTS` block (called `schema`) plus a `runMigrations` function for forward-only `ALTER`s. Adding a column = idempotent `ALTER TABLE … ADD COLUMN IF NOT EXISTS …`. Never `DROP`. Backups go to `<table>_backup_YYYYMMDD` before any curative cleanup migration (see the country cleanup pattern from 2026-05-12).
- **Logging**: `log/slog` text handler everywhere. `-v` flips `slog.LevelDebug` globally in `main.go`. Use `slog.With(...)` for structured fields, not `Printf`-style interpolation.
- **Config**: `internal/config/config.go` supports `${VAR}` and `${VAR:-default}` expansion in YAML. If `config.yaml` is missing, `LoadFromEnv()` builds the config purely from env vars — both paths must stay in sync when adding settings.
- **Feature flags via env**: `BETA_FEATURES=0/1` (foxhound circuit breaker + domain scorer), `SERP_BLOCK_ASSETS=0/1` (asset blocking, off-by-default for risky changes), `COUNTRY_CLEANUP_DRY_RUN=true/false`. Pattern: ship unverified behavioral changes behind off-by-default flags for A/B rather than commit-everything.



