//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/lib/pq"
	"github.com/sadewadee/foxhound/fetch"

	"github.com/sadewadee/serp-scraper/internal/config"
	"github.com/sadewadee/serp-scraper/internal/dedup"
	internalScraper "github.com/sadewadee/serp-scraper/internal/scraper"
)

// ReenrichStage is a minimal autonomous worker that re-enriches business_listings
// rows with a low completeness score. It runs as a continuous loop — no scheduler,
// no Redis queue, no REST trigger. Manual trigger via SQL:
//
//	UPDATE business_listings SET re_enriched_at = NULL WHERE domain IN ('example.com', ...)
//
// Failure modes:
//   - HTTP error (4xx/5xx/network): skip, leave re_enriched_at NULL so next loop retries
//   - Extraction failure (no fields): mark re_enriched_at = NOW() as "tried, nothing there"
//     (permanent dead per user decision — no retries on empty extract)
type ReenrichStage struct {
	cfg   *config.Config
	db    *sql.DB
	dedup *dedup.Store

	processed atomic.Int64
	found     atomic.Int64

	// eligFailures counts consecutive eligibility-query failures across all
	// workers — kept for observability/logging only. It no longer gates health:
	// a transient 57014 statement-timeout under load must not flip the worker
	// unhealthy, because the resulting autoheal restart re-runs Migrate() and
	// re-fires the boot DDL herd, deepening the very contention that caused the
	// timeout (the 2026-06-01 doom loop). Health is progress-based instead.
	eligFailures atomic.Int64

	// lastProgress is the unix-nanos timestamp of the worker's last sign of
	// life: a successful eligibility query (even zero rows — the query ran) or a
	// processed row. healthy() reports degraded only when NO progress has
	// happened for reenrichHealthWindow — that still catches a genuinely stalled
	// worker (issue #28: every query times out, zero rows ever) while ignoring
	// the intermittent timeouts a busy-but-progressing worker sees under load.
	lastProgress atomic.Int64
}

// NewReenrichStage creates a new ReenrichStage.
func NewReenrichStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *ReenrichStage {
	return &ReenrichStage{cfg: cfg, db: database, dedup: dd}
}

// reenrichRow is a candidate row from the eligibility query. Address /
// Country / City are surfaced for the offline pre-pass (applyOfflineParse)
// so we can fill missing geo fields from the existing address blob without
// paying a network refetch. EmailCount / PhoneCount drive the skip-fetch
// decision — a row that already has contact data and just needs geo can
// graduate via offline parse alone.
type reenrichRow struct {
	ID         int64
	Domain     string
	URL        string
	Address    sql.NullString
	Country    sql.NullString
	City       sql.NullString
	EmailCount int64
	PhoneCount int64
}

// Run starts numWorkers goroutines each running the continuous re-enrich loop
// plus one lock-reaper goroutine that deterministically releases stale claims.
// The reaper exists because the worker's eligibility-query stale-claim recovery
// is best-effort: the query scans re_enriched_at IS NULL rows in index order
// and stops at the first LIMIT eligible rows, so a stale lock deep in the pool
// may not resurface for a long time. Rows stuck for hours/days would otherwise
// accumulate; the reaper closes that gap with a deterministic sweep.
func (r *ReenrichStage) Run(ctx context.Context) error {
	numWorkers := r.cfg.ReenrichWorkerCount
	if numWorkers < 1 {
		numWorkers = 1
	}
	slog.Info("reenrich: starting workers", "count", numWorkers, "min_score", r.cfg.ReenrichScore)

	// Seed progress so a freshly-booted worker reports healthy during startup.
	r.recordProgress()

	// Health file so the container healthcheck doesn't kill us while idle.
	// The probe lets the file go stale once eligibility queries have been
	// failing in a row (issue #28) — an idle-but-working reenrich stays healthy,
	// a stalled one (every query timing out) eventually fails its healthcheck.
	go touchHealthFile(ctx, "/tmp/worker-healthy", r.healthy)

	// Lock-leak reaper — deterministic stale-lock release every 5 minutes.
	// Operates independently of worker loops; idempotent UPDATE so multiple
	// reaper instances would be safe but we only spawn one.
	go r.lockReaper(ctx)

	var wg sync.WaitGroup
	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			r.worker(ctx, workerID)
		}(i)
	}
	wg.Wait()
	slog.Info("reenrich: all workers done",
		"processed", r.processed.Load(),
		"found", r.found.Load())
	return nil
}

func (r *ReenrichStage) worker(ctx context.Context, workerID int) {
	host, _ := os.Hostname()
	if len(host) > 12 {
		host = host[:12]
	}
	slog.Info("reenrich: worker starting", "worker", workerID, "host", host)

	// Pool one stealth fetcher per worker — recycle every N requests.
	// Per gotchas.md 2026-04-06: never create+close per request (TLS overhead).
	stealth := internalScraper.NewStealth(r.cfg)
	stealthCount := 0
	stealthRecycleAfter := r.cfg.Fetch.StealthRecycleAfter
	if stealthRecycleAfter <= 0 {
		stealthRecycleAfter = 500
	}
	defer stealth.Close()

	scoreThreshold := r.cfg.ReenrichScore

	for {
		if ctx.Err() != nil {
			return
		}

		// Fetch a batch of eligible rows (randomised to avoid multiple workers
		// clustering on the same rows).
		rows, err := r.fetchEligibleBatch(ctx, scoreThreshold, 100)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			fails := r.eligFailures.Add(1)
			slog.Warn("reenrich: eligibility query failed",
				"worker", workerID, "error", err, "consecutive_failures", fails)
			if fails >= reenrichMaxConsecutiveEligFailures {
				// Operator signal only. Health is progress-based now, so a
				// 57014 statement-timeout under load no longer forces a restart
				// (that restart re-ran Migrate() → boot DDL herd → more
				// contention → the 2026-06-01 doom loop). A genuinely stuck
				// worker (zero rows processed) still goes unhealthy on the
				// reenrichHealthWindow timer.
				slog.Error("reenrich: eligibility query failing repeatedly — DB likely under contention, check pg_stat_activity",
					"worker", workerID, "consecutive_failures", fails)
			}
			// Staggered backoff de-synchronizes the worker fleet so they stop
			// retrying in lockstep and piling onto the same index head via
			// SKIP LOCKED.
			backoff := time.Duration(10+(workerID%6)*2) * time.Second
			select {
			case <-ctx.Done():
				return
			case <-time.After(backoff):
			}
			continue
		}
		// Query succeeded (even if it returned zero rows) — the worker is not
		// stalled, so clear the degraded-health counter.
		r.recordEligibilitySuccess()

		if len(rows) == 0 {
			// No eligible rows — sleep and retry.
			slog.Debug("reenrich: no eligible rows, sleeping", "worker", workerID)
			select {
			case <-ctx.Done():
				return
			case <-time.After(10 * time.Second):
			}
			continue
		}

		for _, row := range rows {
			if ctx.Err() != nil {
				return
			}

			// Recycle stealth fetcher on interval.
			stealthCount++
			if stealthCount >= stealthRecycleAfter {
				stealth.Close()
				stealth = internalScraper.NewStealth(r.cfg)
				stealthCount = 0
				slog.Debug("reenrich: stealth recycled", "worker", workerID)
			}

			r.processRow(ctx, stealth, row, workerID)
		}
	}
}

// reenrichMaxAttempts caps how many times a row can be claimed by the reenrich
// worker before being treated as permanently dead. Cap chosen at 10 — high
// enough to absorb transient fetch/network errors, low enough that genuinely
// broken sites (DNS gone, perpetual 403, etc.) drop out of the eligibility
// pool instead of cycling forever and burning proxy budget.
//
// Mirrors the enrich reconciler cap (15) from .dev-squad/gotchas.md — same
// anti-pattern (zero-reset retry) caused the 2026-04-06 pipeline deadlock.
const reenrichMaxAttempts = 10

// reenrichMaxConsecutiveEligFailures is how many back-to-back eligibility-query
// failures the worker tolerates before the health probe reports degraded.
// Each failure is followed by a 15s back-off, so 5 ≈ 75s of total failure
// before we let the health file go stale — long enough to ride out a transient
// DB blip, short enough that a genuinely stalled worker (issue #28) surfaces to
// autoheal instead of hiding behind a green healthcheck (Operational Invariant
// #7: fail-open paths must signal).
const reenrichMaxConsecutiveEligFailures = 5

// reenrichHealthWindow is how long the worker can go without ANY progress
// (successful eligibility query or processed row) before healthy() reports
// degraded. Generous enough to ride out a transient DB blip; short enough that
// a genuinely stalled worker (issue #28: every query times out, zero rows ever)
// still surfaces to autoheal. A 57014 timeout alone never trips this as long as
// the worker keeps making progress.
const reenrichHealthWindow = 5 * time.Minute

// recordProgress stamps the worker's last sign of life (unix nanos).
func (r *ReenrichStage) recordProgress() { r.lastProgress.Store(time.Now().UnixNano()) }

// recordEligibilitySuccess clears the consecutive-failure counter and records
// progress after any successful eligibility query (including one that returns
// zero eligible rows — the query ran, so the worker is alive and not stalled).
func (r *ReenrichStage) recordEligibilitySuccess() {
	r.eligFailures.Store(0)
	r.recordProgress()
}

// recordEligibilityFailure increments the consecutive-failure counter after an
// eligibility query error (statement timeout, etc.). Observability only — it
// does NOT record progress and does NOT directly gate health.
func (r *ReenrichStage) recordEligibilityFailure() { r.eligFailures.Add(1) }

// healthy reports whether the reenrich worker is doing useful work, keyed on
// recent progress rather than eligibility-query outcomes. A worker that keeps
// processing rows stays healthy even while the eligibility fetch intermittently
// times out under load — which stops the autoheal→Migrate→DDL-herd doom loop
// (2026-06-01). A worker making no progress for reenrichHealthWindow (issue #28)
// reports degraded so touchHealthFile lets the health file go stale and autoheal
// restarts it. A zero lastProgress means "not started yet" → healthy.
func (r *ReenrichStage) healthy() bool {
	lp := r.lastProgress.Load()
	if lp == 0 {
		return true
	}
	return time.Since(time.Unix(0, lp)) < reenrichHealthWindow
}

// eligibilityQuery is the exact SQL fetchEligibleBatch runs, lifted to package
// scope so TestEligibilityQuery_NoOrderByRandom can guard against the ORDER BY
// RANDOM() regression that stalled the reenrich worker (issue #28).
const eligibilityQuery = `
		WITH eligible AS (
			SELECT bl.id
			FROM business_listings bl
			WHERE re_enriched_at IS NULL
			  -- YogaAlliance synthetic domains (<id>.ryt/.rys.yogaalliance.org)
			  -- have no real website: a network refetch yields nothing and they
			  -- can never reach min_score, so they stay permanently eligible and
			  -- dominate the candidate pool — burning 16 workers' cycles for ~0
			  -- new emails (the 2026-06-01 churn). Exclude them; their contact
			  -- data already came from the dedicated crawler.
			  AND bl.domain NOT LIKE '%.ryt.yogaalliance.org'
			  AND bl.domain NOT LIKE '%.rys.yogaalliance.org'
			  AND (re_enrich_locked_at IS NULL
			       OR re_enrich_locked_at < NOW() - INTERVAL '15 minutes')
			  AND COALESCE(bl.re_enrich_attempts, 0) < $3
			  -- Eligibility filters on the PRECOMPUTED completeness_score column
			  -- (0-100, maintained by trg_normalize_enrichment + a one-time backfill)
			  -- instead of the old per-candidate correlated EXISTS completeness CASE,
			  -- which re-scanned business_emails+emails for EVERY re_enriched_at IS NULL
			  -- candidate and blew the 5s statement_timeout under 16-worker contention
			  -- (the 2026-06-02 eligibility storm). Backed by idx_bl_reenrich_score.
			  -- A NULL score (not yet scored) is excluded until the backfill sets it.
			  AND bl.completeness_score < $1
			-- Deliberately NOT randomly ordered: a random sort over the whole
			-- eligible pool forced a full index scan + sort of every
			-- re_enriched_at IS NULL row (~340K), so the SELECT blew the 5s
			-- statement_timeout on every loop and the worker claimed ~0 rows
			-- (issue #28 — reenrich stalled at ~88 rows/hr). Index-order scan +
			-- LIMIT short-circuits after the first $2 eligible rows (~4ms on
			-- prod). Work is still spread across workers/iterations by FOR UPDATE
			-- OF bl SKIP LOCKED plus the 15-min re_enrich_locked_at window, which
			-- advances the scan as claimed rows drop out of the candidate set.
			LIMIT $2
			FOR UPDATE OF bl SKIP LOCKED
		)
		UPDATE business_listings bl
		SET re_enrich_locked_at = NOW(),
		    re_enrich_attempts  = COALESCE(bl.re_enrich_attempts, 0) + 1
		FROM eligible
		WHERE bl.id = eligible.id
		RETURNING
			bl.id,
			bl.domain,
			COALESCE(bl.website, 'https://' || bl.domain),
			bl.address,
			bl.country,
			bl.city,
			(SELECT COUNT(*) FROM business_emails be WHERE be.business_id = bl.id),
			(CASE WHEN bl.phone IS NOT NULL AND bl.phone != '' THEN 1 ELSE 0 END
			 + COALESCE(array_length(bl.phones, 1), 0))
	`

// fetchEligibleBatch atomically claims up to limit business_listings rows.
//
// Multi-worker correctness: uses CTE with FOR UPDATE OF bl SKIP LOCKED to
// prevent two workers from picking the same row. Claim is recorded by
// setting re_enrich_locked_at = NOW() and incrementing re_enrich_attempts
// in the same statement.
//
// Stale-claim recovery: rows with re_enrich_locked_at older than 15 min
// are considered abandoned. The lockReaper goroutine sweeps them out
// deterministically every 5 min; the WHERE here also covers them as a
// secondary safety net for the gap between reaper ticks.
//
// Retry cap: rows with re_enrich_attempts >= reenrichMaxAttempts (10) are
// excluded so genuinely broken sites stop cycling. Manual reset via
// `UPDATE business_listings SET re_enrich_attempts = 0 WHERE domain = ...`
// brings them back if needed.
//
// Score (must be < threshold to be eligible):
//   - 40 pts: has a valid email (is_acceptable=true OR score>=0.7)
//   - 20 pts: phone or phones array non-empty
//   - 15 pts: business_name AND category both non-empty
//   - 15 pts: address non-empty OR (city AND country non-empty)
//   - 10 pts: at least one social link present
//     Total = 100
//
// Statement timeout 5s prevents holding a connection on the 500K-row table
// (per gotchas.md 2026-04-06: collectSnapshot COUNT(*) pattern).
func (r *ReenrichStage) fetchEligibleBatch(ctx context.Context, scoreThreshold int, limit int) ([]reenrichRow, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const q = eligibilityQuery

	tx, err := r.db.BeginTx(queryCtx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(queryCtx, `SET LOCAL statement_timeout = '5000'`); err != nil {
		return nil, err
	}

	dbRows, err := tx.QueryContext(queryCtx, q, scoreThreshold, limit, reenrichMaxAttempts)
	if err != nil {
		return nil, err
	}
	defer dbRows.Close()

	var result []reenrichRow
	for dbRows.Next() {
		var row reenrichRow
		if err := dbRows.Scan(
			&row.ID, &row.Domain, &row.URL,
			&row.Address, &row.Country, &row.City,
			&row.EmailCount, &row.PhoneCount,
		); err != nil {
			slog.Warn("reenrich: scan row failed", "error", err)
			continue
		}
		result = append(result, row)
	}
	if err := dbRows.Err(); err != nil {
		return nil, err
	}
	if err := tx.Commit(); err != nil {
		slog.Warn("reenrich: eligibility tx commit failed", "error", err)
		return nil, err
	}
	return result, nil
}

func (r *ReenrichStage) processRow(ctx context.Context, stealth *fetch.StealthFetcher, row reenrichRow, workerID int) {
	// Claiming + working a row is a sign of life — keep the worker healthy even
	// if the next eligibility fetch times out under load.
	r.recordProgress()

	// Offline pre-pass: derive country/city from the existing address blob
	// before paying for a network refetch. If the row already had contact
	// data (emails/phones) and the offline parse fills in country+city, we
	// can mark it done and skip the fetch entirely.
	if r.applyOfflineParse(ctx, row, workerID) {
		return
	}

	timeout := time.Duration(r.cfg.Enrich.TimeoutMs) * time.Millisecond
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	fetchCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	body, err := fetchStealthOnly(fetchCtx, stealth, row.URL, fmt.Sprintf("reenrich-%d", row.ID))
	if err != nil {
		// HTTP / network error: release the claim immediately so another
		// worker can retry without waiting for the 15-min stale-claim window.
		// re_enriched_at stays NULL → row remains eligible.
		r.releaseLock(ctx, row.ID)
		slog.Debug("reenrich: fetch error, releasing lock", "domain", row.Domain, "error", err, "worker", workerID)
		return
	}

	// Extract contacts from fetched body.
	cd := internalScraper.ExtractContacts([]byte(body))
	internalScraper.ApplyTLDCountryFallback(cd, row.URL)
	rawEmails := internalScraper.FilterEmails(cd.Emails)
	rawPhones := internalScraper.FilterPhones(cd.Phones)

	// Sanitize emails and phones: strip invalid UTF-8 bytes that would cause
	// "pq: invalid byte sequence for encoding UTF8" on the Postgres upsert.
	emails := make([]string, len(rawEmails))
	for i, e := range rawEmails {
		emails[i] = sanitizeUTF8(e)
	}
	phones := make([]string, len(rawPhones))
	for i, p := range rawPhones {
		phones[i] = sanitizeUTF8(p)
	}

	socialLinks := buildSocialLinks(cd) // reuse enrich.go helper — same package
	socialJSON, _ := json.Marshal(socialLinks)

	// Write results by upserting an enrichment_jobs row with status='completed'.
	// The DB trigger (trg_enrich_complete_upsert) handles the upsert into
	// business_listings — we do NOT duplicate that logic here.
	urlHash := dedup.HashURL(row.URL)
	_, insertErr := r.db.ExecContext(ctx, `
		INSERT INTO enrichment_jobs (
			url, url_hash, domain, status,
			raw_emails, raw_phones, raw_social,
			raw_business_name, raw_category, raw_address,
			raw_page_title, raw_description, raw_location, raw_country,
			raw_city, raw_contact_name, raw_opening_hours, raw_rating,
			raw_tiktok, raw_youtube, raw_telegram,
			completed_at, updated_at
		) VALUES (
			$1, $2, $3, 'completed',
			$4, $5, $6,
			$7, $8, $9,
			$10, $11, $12, $13,
			$14, $15, $16, $17,
			$18, $19, $20,
			NOW(), NOW()
		)
		ON CONFLICT (url_hash) DO UPDATE SET
			status        = 'completed',
			raw_emails    = EXCLUDED.raw_emails,
			raw_phones    = EXCLUDED.raw_phones,
			raw_social    = EXCLUDED.raw_social,
			raw_business_name  = COALESCE(EXCLUDED.raw_business_name,  enrichment_jobs.raw_business_name),
			raw_category       = COALESCE(EXCLUDED.raw_category,       enrichment_jobs.raw_category),
			raw_address        = COALESCE(EXCLUDED.raw_address,        enrichment_jobs.raw_address),
			raw_page_title     = COALESCE(EXCLUDED.raw_page_title,     enrichment_jobs.raw_page_title),
			raw_description    = COALESCE(EXCLUDED.raw_description,    enrichment_jobs.raw_description),
			raw_location       = COALESCE(EXCLUDED.raw_location,       enrichment_jobs.raw_location),
			raw_country        = COALESCE(EXCLUDED.raw_country,        enrichment_jobs.raw_country),
			raw_city           = COALESCE(EXCLUDED.raw_city,           enrichment_jobs.raw_city),
			raw_contact_name   = COALESCE(EXCLUDED.raw_contact_name,   enrichment_jobs.raw_contact_name),
			raw_opening_hours  = COALESCE(EXCLUDED.raw_opening_hours,  enrichment_jobs.raw_opening_hours),
			raw_rating         = COALESCE(EXCLUDED.raw_rating,         enrichment_jobs.raw_rating),
			raw_tiktok         = COALESCE(EXCLUDED.raw_tiktok,         enrichment_jobs.raw_tiktok),
			raw_youtube        = COALESCE(EXCLUDED.raw_youtube,        enrichment_jobs.raw_youtube),
			raw_telegram       = COALESCE(EXCLUDED.raw_telegram,       enrichment_jobs.raw_telegram),
			completed_at = NOW(), updated_at = NOW()
	`,
		row.URL, urlHash, row.Domain,
		pq.Array(emails), pq.Array(phones), socialJSON,
		nullIfEmpty(sanitizeUTF8(cd.BusinessName)), nullIfEmpty(sanitizeUTF8(cd.BusinessCategory)), nullIfEmpty(sanitizeUTF8(cd.Address)),
		nullIfEmpty(sanitizeUTF8(cd.PageTitle)), nullIfEmpty(sanitizeUTF8(cd.Description)), nullIfEmpty(sanitizeUTF8(cd.Location)), nullIfEmpty(sanitizeUTF8(cd.Country)),
		nullIfEmpty(sanitizeUTF8(cd.City)), nullIfEmpty(sanitizeUTF8(cd.ContactName)), nullIfEmpty(sanitizeUTF8(cd.OpeningHours)), nullIfEmpty(sanitizeUTF8(cd.Rating)),
		nullIfEmpty(sanitizeUTF8(cd.TikTok)), nullIfEmpty(sanitizeUTF8(cd.YouTube)), nullIfEmpty(sanitizeUTF8(cd.Telegram)),
	)
	if insertErr != nil {
		// DB-side failure (transient). Release the claim so another worker
		// can retry without waiting for the 15-min stale-claim window.
		r.releaseLock(ctx, row.ID)
		slog.Error("reenrich: enrichment_jobs upsert failed",
			"domain", row.Domain, "error", insertErr, "worker", workerID)
		return
	}

	// Mark as processed and clear the claim atomically. Extraction failure
	// (zero fields extracted) is also marked done — per user: "permanent
	// dead, no retries on empty extract".
	_, markErr := r.db.ExecContext(ctx,
		`UPDATE business_listings SET re_enriched_at = NOW(), re_enrich_locked_at = NULL WHERE id = $1`, row.ID)
	if markErr != nil {
		slog.Error("reenrich: failed to mark re_enriched_at",
			"id", row.ID, "domain", row.Domain, "error", markErr)
		// Lock will auto-expire after 15 min; data is already upserted via trigger.
		return
	}

	r.processed.Add(1)
	if len(emails) > 0 {
		r.found.Add(int64(len(emails)))
		slog.Info("reenrich: page done",
			"domain", row.Domain,
			"emails", len(emails),
			"phones", len(phones),
			"worker", workerID)
	}
}

// applyOfflineParse derives country/city from row.Address via the tail-parse
// logic used by ExtractContacts, then writes any newly-derived values back to
// business_listings. If the row already had at least one email/phone AND the
// offline parse produced (or row already had) both country and city, the row
// is marked re_enriched_at = NOW() and the function returns true — caller
// skips the network fetch.
//
// Returns true when the network fetch can be skipped, false when the worker
// should proceed with the normal fetch path. Errors fall through to the
// network path (defensive — better to refetch than lose a row).
func (r *ReenrichStage) applyOfflineParse(ctx context.Context, row reenrichRow, workerID int) bool {
	if !row.Address.Valid || strings.TrimSpace(row.Address.String) == "" {
		return false
	}

	parsedCountry, parsedCity := internalScraper.ParseAddressFallback(row.Address.String)
	newCountry, newCity, skipFetch := offlineParseDecision(row, parsedCountry, parsedCity)

	if newCountry.Valid || newCity.Valid {
		// COALESCE on the SQL side mirrors the trigger's merge semantics: only
		// overwrite when the existing column is NULL. Defensive against races
		// where another worker filled the column between our SELECT and UPDATE.
		_, err := r.db.ExecContext(ctx, `
			UPDATE business_listings
			SET country    = COALESCE(country, $2),
			    city       = COALESCE(city,    $3),
			    updated_at = NOW()
			WHERE id = $1
		`, row.ID, newCountry, newCity)
		if err != nil {
			slog.Warn("reenrich: offline parse update failed, will fall through to fetch",
				"id", row.ID, "domain", row.Domain, "error", err, "worker", workerID)
			return false
		}
		slog.Debug("reenrich: offline parse filled",
			"id", row.ID, "domain", row.Domain,
			"new_country", newCountry.String, "new_city", newCity.String,
			"worker", workerID)
	}

	if !skipFetch {
		return false
	}

	_, err := r.db.ExecContext(ctx, `
		UPDATE business_listings
		SET re_enriched_at = NOW(), re_enrich_locked_at = NULL
		WHERE id = $1
	`, row.ID)
	if err != nil {
		slog.Warn("reenrich: mark done after offline parse failed, falling through to fetch",
			"id", row.ID, "domain", row.Domain, "error", err, "worker", workerID)
		return false
	}
	r.processed.Add(1)
	slog.Info("reenrich: completed via offline parse (no network)",
		"id", row.ID, "domain", row.Domain,
		"filled_country", newCountry.Valid, "filled_city", newCity.Valid,
		"worker", workerID)
	return true
}

// offlineParseDecision is the pure-logic core of applyOfflineParse — given
// the row's current state and the parsed (country, city) values from
// scraper.ParseAddressFallback, it decides what to write back and whether
// the network fetch can be skipped. No I/O; trivially unit-testable.
func offlineParseDecision(row reenrichRow, parsedCountry, parsedCity string) (newCountry, newCity sql.NullString, skipFetch bool) {
	if (!row.Country.Valid || row.Country.String == "") && parsedCountry != "" {
		newCountry = sql.NullString{String: parsedCountry, Valid: true}
	}
	if (!row.City.Valid || row.City.String == "") && parsedCity != "" {
		newCity = sql.NullString{String: parsedCity, Valid: true}
	}
	hasContact := row.EmailCount > 0 || row.PhoneCount > 0
	countryFinal := (row.Country.Valid && row.Country.String != "") || newCountry.Valid
	cityFinal := (row.City.Valid && row.City.String != "") || newCity.Valid
	skipFetch = hasContact && countryFinal && cityFinal
	return
}

// releaseLock clears re_enrich_locked_at so another worker can immediately
// re-attempt the row. Used after fetch/insert errors that don't warrant
// marking the row done. Errors here are non-fatal — the 15-min stale-claim
// window in fetchEligibleBatch will recover the row anyway.
func (r *ReenrichStage) releaseLock(ctx context.Context, id int64) {
	_, err := r.db.ExecContext(ctx,
		`UPDATE business_listings SET re_enrich_locked_at = NULL WHERE id = $1`, id)
	if err != nil {
		slog.Debug("reenrich: release lock failed (will auto-expire)", "id", id, "error", err)
	}
}

// lockReaper releases stale re_enrich_locked_at claims deterministically on
// a 5-minute tick. Without this, the eligibility query (which scans in index
// order and stops at the LIMIT) may not revisit specific stuck rows for a long
// time — production audit (2026-05-12) found 708 rows stale-locked, oldest 3
// days. The reaper closes that gap.
//
// Idempotent — clearing a non-stale lock is a no-op. Safe to run alongside
// workers; the 15-min threshold is much longer than max processRow timeout
// (30s default), so we cannot race against an in-flight worker.
func (r *ReenrichStage) lockReaper(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Minute)
	defer ticker.Stop()

	// Run once at startup to clear any existing stale locks from a prior
	// crash before workers begin claiming. Production audit found this
	// snapshot can be hundreds of rows.
	r.reapStaleLocks(ctx)

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			r.reapStaleLocks(ctx)
		}
	}
}

// reapStaleLocks performs one reaper sweep — released rows go back into the
// eligibility pool on the next worker batch. Tight context timeout so a slow
// DB doesn't pile up overlapping reaper UPDATEs.
func (r *ReenrichStage) reapStaleLocks(ctx context.Context) {
	queryCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	res, err := r.db.ExecContext(queryCtx, `
		UPDATE business_listings
		SET re_enrich_locked_at = NULL
		WHERE re_enrich_locked_at IS NOT NULL
		  AND re_enrich_locked_at < NOW() - INTERVAL '15 minutes'
		  AND re_enriched_at IS NULL
	`)
	if err != nil {
		slog.Warn("reenrich: lock reaper sweep failed", "error", err)
		return
	}
	n, _ := res.RowsAffected()
	if n > 0 {
		slog.Info("reenrich: lock reaper released stale claims", "released", n)
	}
}

// Processed returns the total rows marked re_enriched_at by this stage instance.
func (r *ReenrichStage) Processed() int64 { return r.processed.Load() }

// Found returns the total valid emails found by this stage instance.
func (r *ReenrichStage) Found() int64 { return r.found.Load() }

// sanitizeUTF8 strips any byte sequences that are not valid UTF-8 from s.
// Applied to every string that originates from scraped page content before it
// is written into Postgres — prevents "pq: invalid byte sequence for encoding
// UTF8" upsert failures on pages with mixed/binary encodings (e.g. windows-1252
// pages that slipped through the response-body decoder).
func sanitizeUTF8(s string) string { return strings.ToValidUTF8(s, "") }
