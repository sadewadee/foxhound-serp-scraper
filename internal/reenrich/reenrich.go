// Package reenrich provides selective re-enrichment for business_listings rows
// whose original enrichment failed to capture the product-critical signal
// (a valid email). It targets bandwidth at the rows where re-fetching has the
// highest yield — domains with no valid email after >14 days — and skips
// rows that just lack optional metadata (opening_hours, rating).
//
// Three entry points:
//
//	EligibleDomains  — pure SQL query, returns candidates.
//	EnqueueDomains   — fan-out each candidate into N contact paths.
//	RunScheduler     — background ticker for autonomous re-enrich.
//
// The HTTP handler in internal/api wires the same primitives for on-demand
// admin-triggered runs.
package reenrich

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"log/slog"
	"strings"
	"time"

	"github.com/lib/pq"
)

// ContactPaths is the global multi-language sweep used by re-enrichment.
// Includes EN/DE/ES/IT/FR/PL/PT variants so non-English sites are reached.
// Intentionally generous — we are scraping the entire world, not just .com.
var ContactPaths = []string{
	"/contact",
	"/contact-us",
	"/about",
	"/about-us",
	"/team",
	"/our-team",
	"/staff",
	"/people",
	"/imprint",
	"/impressum",      // German legal contact page (mandatory in DE/AT/CH)
	"/kontakt",        // German/Polish/Norwegian
	"/contacto",       // Spanish/Portuguese
	"/contatti",       // Italian
	"/nous-contacter", // French
	"/contactez-nous", // French
}

// FilterMode controls which rows are eligible for re-enrich.
type FilterMode string

const (
	// FilterNoValidEmail re-enriches rows that have no email validated as
	// 'valid'. Most useful — addresses the actual product-value gap.
	FilterNoValidEmail FilterMode = "no_valid_email"
	// FilterNoEmail re-enriches rows that have ZERO emails (not even invalid).
	// Stricter than NoValidEmail.
	FilterNoEmail FilterMode = "no_email"
	// FilterSpecificDomains re-enriches an explicit caller-supplied domain list.
	// Domains parameter must be non-empty.
	FilterSpecificDomains FilterMode = "specific_domains"
)

// Options drives a re-enrich run.
type Options struct {
	Filter        FilterMode
	Domains       []string // only used when Filter == FilterSpecificDomains
	MaxAgeDays    int      // skip rows newer than this
	GapDays       int      // skip domains re-enriched within this many days
	Limit         int      // 0 = unlimited (per "scraping seluruh dunia, jangan dibatasi")
	ExtraPaths    []string // append to ContactPaths
	ParentQueryID int64    // optional, for telemetry / lineage
}

// DefaultOptions returns conservative re-enrich defaults: targets
// no-valid-email rows older than 14 days, with a 7-day gap to avoid
// thrashing the same domains.
func DefaultOptions() Options {
	return Options{
		Filter:     FilterNoValidEmail,
		MaxAgeDays: 14,
		GapDays:    7,
		Limit:      0, // unlimited by default
	}
}

// Domain identifies a re-enrichable target.
type Domain struct {
	Domain string
	URL    string // last-known URL for the domain (used as fallback if path sweep finds nothing)
}

// EligibleDomains returns candidates that match the filter without enqueuing
// anything. Safe to call repeatedly — this is a read-only query.
func EligibleDomains(ctx context.Context, db *sql.DB, opts Options) ([]Domain, error) {
	if opts.Filter == FilterSpecificDomains {
		return specificDomainList(ctx, db, opts)
	}

	emailJoin := ""
	emailWhere := ""
	switch opts.Filter {
	case FilterNoValidEmail:
		emailJoin = `LEFT JOIN business_emails be ON be.business_id = bl.id
		             LEFT JOIN emails e ON e.id = be.email_id AND e.validation_status = 'valid'`
		emailWhere = `e.id IS NULL`
	case FilterNoEmail:
		emailJoin = `LEFT JOIN business_emails be ON be.business_id = bl.id`
		emailWhere = `be.business_id IS NULL`
	default:
		return nil, fmt.Errorf("reenrich: unknown filter %q", opts.Filter)
	}

	maxAge := opts.MaxAgeDays
	if maxAge <= 0 {
		maxAge = 14
	}
	gap := opts.GapDays
	if gap <= 0 {
		gap = 7
	}

	limitClause := ""
	if opts.Limit > 0 {
		limitClause = fmt.Sprintf("LIMIT %d", opts.Limit)
	}

	q := fmt.Sprintf(`
		SELECT DISTINCT bl.domain, bl.url
		FROM business_listings bl
		%s
		WHERE %s
		  AND bl.created_at < NOW() - INTERVAL '%d days'
		  AND NOT EXISTS (
		    SELECT 1 FROM enrichment_jobs ej
		    WHERE ej.domain = bl.domain
		      AND ej.status IN ('pending', 'processing')
		      AND ej.created_at > NOW() - INTERVAL '%d days'
		  )
		ORDER BY bl.created_at DESC
		%s
	`, emailJoin, emailWhere, maxAge, gap, limitClause)

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, fmt.Errorf("reenrich: query eligibles: %w", err)
	}
	defer rows.Close()

	var out []Domain
	for rows.Next() {
		var d Domain
		if err := rows.Scan(&d.Domain, &d.URL); err != nil {
			slog.Warn("reenrich: scan eligible row", "error", err)
			continue
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

func specificDomainList(ctx context.Context, db *sql.DB, opts Options) ([]Domain, error) {
	if len(opts.Domains) == 0 {
		return nil, fmt.Errorf("reenrich: specific_domains filter requires at least one domain")
	}
	rows, err := db.QueryContext(ctx, `
		SELECT domain, url FROM business_listings WHERE domain = ANY($1)
	`, pq.Array(opts.Domains))
	if err != nil {
		return nil, fmt.Errorf("reenrich: query specific domains: %w", err)
	}
	defer rows.Close()
	var out []Domain
	for rows.Next() {
		var d Domain
		if err := rows.Scan(&d.Domain, &d.URL); err != nil {
			continue
		}
		out = append(out, d)
	}
	return out, rows.Err()
}

// EnqueueDomains fans out each domain into ContactPaths + ExtraPaths
// enrichment jobs, deduped by url_hash. Returns the count actually inserted
// (existing url_hash collisions are skipped via ON CONFLICT DO NOTHING).
func EnqueueDomains(ctx context.Context, db *sql.DB, domains []Domain, opts Options) (int, error) {
	if len(domains) == 0 {
		return 0, nil
	}

	paths := append([]string{}, ContactPaths...)
	paths = append(paths, opts.ExtraPaths...)

	// Single multi-row INSERT per batch keeps the round-trip cost flat.
	// Batch size 500 keeps the parameter list under PG's 65535 limit even
	// at the widest path expansion (500 × 16 paths = 8000 rows per batch).
	const batchSize = 500
	totalQueued := 0

	for i := 0; i < len(domains); i += batchSize {
		end := i + batchSize
		if end > len(domains) {
			end = len(domains)
		}
		batch := domains[i:end]

		urls := make([]string, 0, len(batch)*len(paths))
		hashes := make([]string, 0, cap(urls))
		batchDomains := make([]string, 0, cap(urls))
		for _, d := range batch {
			for _, p := range paths {
				url := "https://" + d.Domain + p
				h := sha256.Sum256([]byte(url))
				urls = append(urls, url)
				hashes = append(hashes, hex.EncodeToString(h[:]))
				batchDomains = append(batchDomains, d.Domain)
			}
		}

		res, err := db.ExecContext(ctx, `
			INSERT INTO enrichment_jobs (url, url_hash, domain, parent_query_id, source, status)
			SELECT u.url, u.url_hash, u.domain, $4, 'reenrich', 'pending'
			FROM unnest($1::text[], $2::text[], $3::text[]) AS u(url, url_hash, domain)
			ON CONFLICT (url_hash) DO NOTHING
		`, pq.Array(urls), pq.Array(hashes), pq.Array(batchDomains), nullIfZero(opts.ParentQueryID))
		if err != nil {
			slog.Warn("reenrich: batch enqueue failed", "error", err, "batch_start", i)
			continue
		}
		n, _ := res.RowsAffected()
		totalQueued += int(n)
	}

	slog.Info("reenrich: enqueued",
		"domains", len(domains),
		"paths_per_domain", len(paths),
		"jobs_queued", totalQueued,
		"filter", string(opts.Filter),
	)
	return totalQueued, nil
}

func nullIfZero(n int64) any {
	if n == 0 {
		return nil
	}
	return n
}

// RunScheduler executes EligibleDomains + EnqueueDomains on a fixed interval.
// Honors REENRICH_ENABLED env var (kill switch) and an off-peak window so it
// doesn't compete with normal SERP/enrich load during business hours.
//
// Cancellation: respects ctx; the in-flight batch may complete before the
// goroutine exits.
func RunScheduler(ctx context.Context, db *sql.DB, cfg SchedulerConfig) {
	if !cfg.Enabled {
		slog.Info("reenrich: scheduler disabled (REENRICH_ENABLED!=1)")
		return
	}

	interval := cfg.Interval
	if interval <= 0 {
		interval = 6 * time.Hour
	}

	slog.Info("reenrich: scheduler started",
		"interval", interval.String(),
		"daily_limit", cfg.DailyLimit,
		"offpeak_start_hour", cfg.OffPeakStartHour,
		"offpeak_end_hour", cfg.OffPeakEndHour,
	)

	tick := time.NewTicker(interval)
	defer tick.Stop()

	// Run once at startup to bootstrap, then on the ticker.
	c := make(chan time.Time, 1)
	c <- time.Now()
	for {
		select {
		case <-ctx.Done():
			slog.Info("reenrich: scheduler stopping")
			return
		case t := <-tick.C:
			runOnce(ctx, db, cfg, t)
		case t := <-c:
			runOnce(ctx, db, cfg, t)
			c = nil // bootstrap fires only once
		}
	}
}

// SchedulerConfig holds the cron-style options for the autonomous runner.
type SchedulerConfig struct {
	Enabled          bool
	Interval         time.Duration
	DailyLimit       int // cap on jobs queued per UTC day; 0 = unlimited
	OffPeakStartHour int // local-server hour 0-23, inclusive
	OffPeakEndHour   int // local-server hour 0-23, exclusive; same as start = always on
}

func runOnce(ctx context.Context, db *sql.DB, cfg SchedulerConfig, now time.Time) {
	if !inOffPeakWindow(now, cfg.OffPeakStartHour, cfg.OffPeakEndHour) {
		slog.Debug("reenrich: outside off-peak window, skipping",
			"hour", now.Hour(),
			"window", fmt.Sprintf("%d-%d", cfg.OffPeakStartHour, cfg.OffPeakEndHour),
		)
		return
	}

	opts := DefaultOptions()
	if cfg.DailyLimit > 0 {
		// Check how many we already queued today; cap remaining.
		var queuedToday int
		_ = db.QueryRowContext(ctx, `
			SELECT COUNT(*) FROM enrichment_jobs
			WHERE source = 'reenrich' AND created_at >= date_trunc('day', NOW())
		`).Scan(&queuedToday)
		remaining := cfg.DailyLimit - queuedToday
		if remaining <= 0 {
			slog.Info("reenrich: daily limit reached, skipping run", "limit", cfg.DailyLimit, "queued", queuedToday)
			return
		}
		opts.Limit = remaining
	}

	domains, err := EligibleDomains(ctx, db, opts)
	if err != nil {
		slog.Warn("reenrich: scheduled run failed at eligibility query", "error", err)
		return
	}
	if len(domains) == 0 {
		slog.Info("reenrich: no eligible domains, sleeping")
		return
	}
	if _, err := EnqueueDomains(ctx, db, domains, opts); err != nil {
		slog.Warn("reenrich: scheduled enqueue failed", "error", err)
	}
}

// inOffPeakWindow returns true when `start == end` (always on) or when `now.Hour()`
// falls in [start, end). Handles wrap-around (e.g. 22 → 6 = night).
func inOffPeakWindow(now time.Time, start, end int) bool {
	if start == end {
		return true
	}
	h := now.Hour()
	if start < end {
		return h >= start && h < end
	}
	// Wrap-around window (22 → 6 spans midnight)
	return h >= start || h < end
}

// MustString trims and returns an env-style string. Used by callers reading
// scheduler config from cfg.
func MustString(s string) string { return strings.TrimSpace(s) }
