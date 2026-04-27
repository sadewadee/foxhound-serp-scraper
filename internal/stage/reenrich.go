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

// Run starts numWorkers goroutines each running the continuous re-enrich loop.
func (r *ReenrichStage) Run(ctx context.Context) error {
	numWorkers := r.cfg.ReenrichWorkerCount
	if numWorkers < 1 {
		numWorkers = 1
	}
	slog.Info("reenrich: starting workers", "count", numWorkers, "min_score", r.cfg.ReenrichScore)

	// Health file so the container healthcheck doesn't kill us while idle.
	go touchHealthFile(ctx, "/tmp/worker-healthy")

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

// fetchEligibleBatch returns up to limit business_listings rows where
// re_enriched_at IS NULL and the inline completeness score < threshold.
//
// Scoring is computed inline in SQL so we avoid pulling columns not needed and
// let PG filter before transferring data. The score mirrors the rubric:
//   - 40 pts: has a valid email (is_acceptable=true OR score>=0.7 in emails table)
//   - 20 pts: phone or phones array non-empty
//   - 15 pts: business_name AND category both non-empty
//   - 15 pts: address non-empty OR (city AND country non-empty)
//   - 10 pts: at least one social link present
//     Total = 100
//
// ORDER BY RANDOM() ensures even distribution across concurrent workers.
// Statement timeout 5s prevents holding a connection on the 500K-row table
// (per gotchas.md 2026-04-06: collectSnapshot COUNT(*) pattern).
func (r *ReenrichStage) fetchEligibleBatch(ctx context.Context, scoreThreshold int, limit int) ([]reenrichRow, error) {
	queryCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	const q = `
		SELECT id, domain, COALESCE(website, 'https://' || domain) AS url
		FROM business_listings bl
		WHERE re_enriched_at IS NULL
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
	`

	// Use SET LOCAL statement_timeout so this single query can't hold a
	// connection indefinitely on a large table. The transaction wraps only
	// the SELECT; we commit immediately after.
	tx, err := r.db.BeginTx(queryCtx, nil)
	if err != nil {
		return nil, err
	}
	defer tx.Rollback() //nolint:errcheck

	if _, err := tx.ExecContext(queryCtx, `SET LOCAL statement_timeout = '5000'`); err != nil {
		return nil, err
	}

	dbRows, err := tx.QueryContext(queryCtx, q, scoreThreshold, limit)
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
	if err := tx.Commit(); err != nil {
		slog.Warn("reenrich: eligibility tx commit failed", "error", err)
	}
	return result, dbRows.Err()
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
		// HTTP / network error: skip, leave re_enriched_at NULL so the next
		// loop iteration will retry. HTTP errors do NOT trigger permanent dead.
		slog.Debug("reenrich: fetch error, skipping", "domain", row.Domain, "error", err, "worker", workerID)
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
		// Don't mark re_enriched_at — will retry on next loop iteration.
		slog.Error("reenrich: enrichment_jobs upsert failed",
			"domain", row.Domain, "error", insertErr, "worker", workerID)
		return
	}

	// Mark as processed. Extraction failure (zero fields extracted) is also
	// marked done — per user: "permanent dead, no retries on empty extract".
	_, markErr := r.db.ExecContext(ctx,
		`UPDATE business_listings SET re_enriched_at = NOW() WHERE id = $1`, row.ID)
	if markErr != nil {
		slog.Error("reenrich: failed to mark re_enriched_at",
			"id", row.ID, "domain", row.Domain, "error", markErr)
		// Row remains eligible (re_enriched_at still NULL), retry next loop.
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

// Processed returns the total rows marked re_enriched_at by this stage instance.
func (r *ReenrichStage) Processed() int64 { return r.processed.Load() }

// Found returns the total valid emails found by this stage instance.
func (r *ReenrichStage) Found() int64 { return r.found.Load() }
