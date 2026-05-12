//go:build playwright

package stage

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
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
}

// NewReenrichStage creates a new ReenrichStage.
func NewReenrichStage(cfg *config.Config, database *sql.DB, dd *dedup.Store) *ReenrichStage {
	return &ReenrichStage{cfg: cfg, db: database, dedup: dd}
}

// reenrichRow is a candidate row from the eligibility query.
type reenrichRow struct {
	ID     int64
	Domain string
	URL    string
}

// Run starts numWorkers goroutines each running the continuous re-enrich loop
// plus one lock-reaper goroutine that deterministically releases stale claims.
// The reaper exists because the worker's eligibility-query stale-claim recovery
// is probabilistic (ORDER BY RANDOM over a 261K-row pool) — a row's specific
// stale lock has 0.04% chance of being re-picked per batch, so rows stuck for
// hours/days accumulate. The reaper closes that gap with a deterministic sweep.
func (r *ReenrichStage) Run(ctx context.Context) error {
	numWorkers := r.cfg.ReenrichWorkerCount
	if numWorkers < 1 {
		numWorkers = 1
	}
	slog.Info("reenrich: starting workers", "count", numWorkers, "min_score", r.cfg.ReenrichScore)

	// Health file so the container healthcheck doesn't kill us while idle.
	go touchHealthFile(ctx, "/tmp/worker-healthy")

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
			slog.Warn("reenrich: eligibility query failed", "worker", workerID, "error", err)
			select {
			case <-ctx.Done():
				return
			case <-time.After(15 * time.Second):
			}
			continue
		}

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

	const q = `
		WITH eligible AS (
			SELECT bl.id
			FROM business_listings bl
			WHERE re_enriched_at IS NULL
			  AND (re_enrich_locked_at IS NULL
			       OR re_enrich_locked_at < NOW() - INTERVAL '15 minutes')
			  AND COALESCE(bl.re_enrich_attempts, 0) < $3
			  AND (
				CASE WHEN EXISTS(
					SELECT 1 FROM business_emails be
					JOIN emails e ON e.id = be.email_id
					WHERE be.business_id = bl.id
					  AND (e.is_acceptable = true OR e.score >= 0.7)
				) THEN 40 ELSE 0 END
				+
				CASE WHEN (bl.phone IS NOT NULL AND bl.phone != '')
					OR (bl.phones IS NOT NULL AND array_length(bl.phones, 1) > 0)
				THEN 20 ELSE 0 END
				+
				CASE WHEN (bl.business_name IS NOT NULL AND bl.business_name != '')
					AND (bl.category IS NOT NULL AND bl.category != '')
				THEN 15 ELSE 0 END
				+
				CASE WHEN (bl.address IS NOT NULL AND bl.address != '')
					OR ((bl.city IS NOT NULL AND bl.city != '') AND (bl.country IS NOT NULL AND bl.country != ''))
				THEN 15 ELSE 0 END
				+
				CASE WHEN bl.social_links IS NOT NULL AND bl.social_links != '{}'::jsonb
				THEN 10 ELSE 0 END
			  ) < $1
			ORDER BY RANDOM()
			LIMIT $2
			FOR UPDATE OF bl SKIP LOCKED
		)
		UPDATE business_listings bl
		SET re_enrich_locked_at = NOW(),
		    re_enrich_attempts  = COALESCE(bl.re_enrich_attempts, 0) + 1
		FROM eligible
		WHERE bl.id = eligible.id
		RETURNING bl.id, bl.domain, COALESCE(bl.website, 'https://' || bl.domain)
	`

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
		if err := dbRows.Scan(&row.ID, &row.Domain, &row.URL); err != nil {
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
	emails := internalScraper.FilterEmails(cd.Emails)
	phones := internalScraper.FilterPhones(cd.Phones)
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
		nullIfEmpty(cd.BusinessName), nullIfEmpty(cd.BusinessCategory), nullIfEmpty(cd.Address),
		nullIfEmpty(cd.PageTitle), nullIfEmpty(cd.Description), nullIfEmpty(cd.Location), nullIfEmpty(cd.Country),
		nullIfEmpty(cd.City), nullIfEmpty(cd.ContactName), nullIfEmpty(cd.OpeningHours), nullIfEmpty(cd.Rating),
		nullIfEmpty(cd.TikTok), nullIfEmpty(cd.YouTube), nullIfEmpty(cd.Telegram),
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
// a 5-minute tick. Without this, the probabilistic ORDER BY RANDOM eligibility
// query rarely re-rolls specific stuck rows — production audit (2026-05-12)
// found 708 rows stale-locked, oldest 3 days. The reaper closes that gap.
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
