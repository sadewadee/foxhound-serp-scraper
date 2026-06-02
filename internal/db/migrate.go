package db

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
)

const schema = `
CREATE TABLE IF NOT EXISTS queries (
    id              BIGSERIAL PRIMARY KEY,
    text            TEXT NOT NULL,
    text_hash       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    country         TEXT DEFAULT '',
    result_count    INTEGER DEFAULT 0,
    error_msg       TEXT,
    expanded_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(text_hash)
);
CREATE INDEX IF NOT EXISTS idx_queries_status ON queries(status);
CREATE INDEX IF NOT EXISTS idx_queries_expand ON queries(status, expanded_at) WHERE status = 'completed' AND expanded_at IS NULL;

CREATE TABLE IF NOT EXISTS serp_jobs (
    id              TEXT PRIMARY KEY,
    parent_job_id   BIGINT NOT NULL REFERENCES queries(id),
    priority        INTEGER DEFAULT 0,
    search_url      TEXT NOT NULL,
    page_num        INTEGER NOT NULL,
    engine          TEXT DEFAULT 'google',
    status          TEXT NOT NULL DEFAULT 'new',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    next_attempt_at TIMESTAMPTZ,
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,
    picked_at       TIMESTAMPTZ,
    result_count    INTEGER DEFAULT 0,
    error_msg       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_serp_jobs_claim ON serp_jobs(parent_job_id, status, priority DESC, created_at) WHERE status = 'new';
CREATE INDEX IF NOT EXISTS idx_serp_jobs_parent ON serp_jobs(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_serp_jobs_status ON serp_jobs(status);
CREATE INDEX IF NOT EXISTS idx_serp_engine ON serp_jobs(engine, status);
CREATE INDEX IF NOT EXISTS idx_serp_jobs_updated_at ON serp_jobs(updated_at) WHERE status = 'completed';
CREATE INDEX IF NOT EXISTS idx_serp_locked ON serp_jobs(locked_at) WHERE status = 'processing';
-- idx_serp_feed and idx_serp_stale created in runMigrations (after ALTER ADD COLUMN picked_at).

CREATE TABLE IF NOT EXISTS serp_results (
    id              BIGSERIAL PRIMARY KEY,
    url             TEXT NOT NULL,
    url_hash        TEXT NOT NULL,
    domain          TEXT NOT NULL,
    source_query_id BIGINT REFERENCES queries(id),
    source_serp_id  TEXT REFERENCES serp_jobs(id),
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(url_hash)
);

CREATE TABLE IF NOT EXISTS enrichment_jobs (
    id              UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    url             TEXT NOT NULL,
    url_hash        TEXT NOT NULL,
    domain          TEXT NOT NULL,
    parent_query_id BIGINT REFERENCES queries(id),
    source          TEXT NOT NULL DEFAULT 'serp_result',
    status          TEXT NOT NULL DEFAULT 'pending',
    attempt_count   INTEGER DEFAULT 0,
    max_attempts    INTEGER DEFAULT 5,
    next_attempt_at TIMESTAMPTZ,
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,
    picked_at       TIMESTAMPTZ,
    error_msg       TEXT,
    raw_emails      TEXT[] DEFAULT '{}',
    raw_phones      TEXT[] DEFAULT '{}',
    raw_social      JSONB DEFAULT '{}',
    raw_business_name TEXT,
    raw_category    TEXT,
    raw_address     TEXT,
    raw_page_title  TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    completed_at    TIMESTAMPTZ,
    UNIQUE(url_hash)
);
CREATE INDEX IF NOT EXISTS idx_enrich_feed ON enrichment_jobs(created_at) WHERE status='pending' AND picked_at IS NULL;
CREATE INDEX IF NOT EXISTS idx_enrich_stale ON enrichment_jobs(picked_at) WHERE status='pending' AND picked_at IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_enrich_status ON enrichment_jobs(status);
CREATE INDEX IF NOT EXISTS idx_enrich_locked ON enrichment_jobs(locked_at) WHERE status = 'processing';
CREATE INDEX IF NOT EXISTS idx_enrich_domain ON enrichment_jobs(domain);
CREATE INDEX IF NOT EXISTS idx_enrich_parent ON enrichment_jobs(parent_query_id);
CREATE INDEX IF NOT EXISTS idx_enrich_completed_at ON enrichment_jobs(completed_at) WHERE status = 'completed';

CREATE TABLE IF NOT EXISTS business_listings (
    id              BIGSERIAL PRIMARY KEY,
    domain          TEXT NOT NULL UNIQUE,
    url             TEXT NOT NULL,
    business_name   TEXT,
    category        TEXT,
    description     TEXT,
    address         TEXT,
    location        TEXT,
    phone           TEXT,
    website         TEXT,
    page_title      TEXT,
    social_links    JSONB DEFAULT '{}',
    opening_hours   TEXT,
    rating          TEXT,
    source_query_id BIGINT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS emails (
    id                BIGSERIAL PRIMARY KEY,
    email             TEXT NOT NULL UNIQUE,
    domain            TEXT,
    local_part        TEXT,
    validation_status TEXT DEFAULT 'pending',
    mx_valid          BOOLEAN,
    deliverable       BOOLEAN,
    disposable        BOOLEAN,
    role_account      BOOLEAN,
    free_email        BOOLEAN,
    catch_all         BOOLEAN,
    reason            TEXT,
    score             REAL,
    is_acceptable     BOOLEAN,
    validated_at      TIMESTAMPTZ,
    created_at        TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_emails_domain ON emails(domain);
CREATE INDEX IF NOT EXISTS idx_emails_validation ON emails(validation_status);

CREATE TABLE IF NOT EXISTS business_emails (
    business_id BIGINT REFERENCES business_listings(id) ON DELETE CASCADE,
    email_id    BIGINT REFERENCES emails(id) ON DELETE CASCADE,
    source      TEXT DEFAULT 'enrichment',
    created_at  TIMESTAMPTZ DEFAULT NOW(),
    PRIMARY KEY (business_id, email_id)
);

CREATE TABLE IF NOT EXISTS workers (
    worker_id       TEXT PRIMARY KEY,
    worker_type     TEXT NOT NULL,
    container_id    TEXT,
    status          TEXT DEFAULT 'idle',
    current_job_id  TEXT,
    current_url     TEXT,
    pages_processed BIGINT DEFAULT 0,
    emails_found    BIGINT DEFAULT 0,
    errors_count    BIGINT DEFAULT 0,
    last_heartbeat  TIMESTAMPTZ DEFAULT NOW(),
    started_at      TIMESTAMPTZ DEFAULT NOW()
);
`

// runMigrations applies incremental schema changes that are safe to re-run.
func runMigrations(db *sql.DB) error {
	// Enable pgcrypto for SHA-256 hashing in triggers.
	if _, err := db.Exec(`CREATE EXTENSION IF NOT EXISTS pgcrypto`); err != nil {
		return fmt.Errorf("db: create extension pgcrypto: %w", err)
	}

	// GUARDRAIL: Legacy tables renamed to _backup, NEVER dropped.
	// Incident 2026-04-03: DROP TABLE destroyed 344K emails. Never again.
	db.Exec(`ALTER TABLE IF EXISTS enrich_jobs RENAME TO enrich_jobs_backup`)
	db.Exec(`ALTER TABLE IF EXISTS websites RENAME TO websites_backup`)

	// Add picked_at column to serp_jobs if missing (from redesign).
	if _, err := db.Exec(`ALTER TABLE serp_jobs ADD COLUMN IF NOT EXISTS picked_at TIMESTAMPTZ`); err != nil {
		return fmt.Errorf("db: add picked_at column: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_serp_feed ON serp_jobs(created_at) WHERE status = 'new' AND picked_at IS NULL`); err != nil {
		return fmt.Errorf("db: create idx_serp_feed: %w", err)
	}
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_serp_stale ON serp_jobs(picked_at) WHERE status = 'new' AND picked_at IS NOT NULL`); err != nil {
		return fmt.Errorf("db: create idx_serp_stale: %w", err)
	}

	// Index for reconciler: retire exhausted failed jobs to 'dead' and resurrect viable ones.
	if _, err := db.Exec(`CREATE INDEX IF NOT EXISTS idx_serp_failed_retry ON serp_jobs(updated_at) WHERE status = 'failed'`); err != nil {
		return fmt.Errorf("db: create idx_serp_failed_retry: %w", err)
	}

	// V2 API performance indexes.
	// pg_trgm extension for ILIKE search index.
	db.Exec(`CREATE EXTENSION IF NOT EXISTS pg_trgm`)
	for _, stmt := range []string{
		// emails.created_at: used by stats last_hour/last_24h queries (noted in gotchas.md 2026-04-06).
		`CREATE INDEX IF NOT EXISTS idx_emails_created_at ON emails(created_at)`,
		// business_listings.business_name: used by V2 search filter (ILIKE).
		// pg_trgm GIN index supports ILIKE efficiently.
		`CREATE INDEX IF NOT EXISTS idx_bl_name_trgm ON business_listings USING gin(business_name gin_trgm_ops)`,
		// business_listings.phone: used by V2 stats with_phone count.
		`CREATE INDEX IF NOT EXISTS idx_bl_phone ON business_listings(phone) WHERE phone IS NOT NULL AND phone != ''`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			// pg_trgm might not be available on some setups; log and continue.
			slog.Warn("db: optional index creation", "error", err)
		}
	}

	// Populate emails.domain and emails.local_part for rows that have NULLs.
	if _, err := db.Exec(`UPDATE emails SET domain = split_part(email, '@', 2), local_part = split_part(email, '@', 1) WHERE domain IS NULL AND email LIKE '%@%'`); err != nil {
		return fmt.Errorf("db: backfill email domain/local_part: %w", err)
	}

	// Add delta tracking columns to workers for per-heartbeat rate calculation.
	for _, stmt := range []string{
		`ALTER TABLE workers ADD COLUMN IF NOT EXISTS pages_prev BIGINT DEFAULT 0`,
		`ALTER TABLE workers ADD COLUMN IF NOT EXISTS emails_prev BIGINT DEFAULT 0`,
		`ALTER TABLE workers ADD COLUMN IF NOT EXISTS pages_delta INT DEFAULT 0`,
		`ALTER TABLE workers ADD COLUMN IF NOT EXISTS emails_delta INT DEFAULT 0`,
		`ALTER TABLE workers ADD COLUMN IF NOT EXISTS delta_at TIMESTAMPTZ`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("db: worker delta columns: %w", err)
		}
	}

	// Schema fix 2026-04-27: extraction layer captured fields that the trigger
	// hardcoded to NULL (description, location) or never had a column for
	// (country, city, contact_name, opening_hours, rating, multi-phone).
	// Add missing columns + raw_* counterparts; the trigger update later in
	// this file forwards them. opening_hours/rating remain optional — they
	// stay null when extraction misses them.
	for _, stmt := range []string{
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS country TEXT`,
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS city TEXT`,
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS contact_name TEXT`,
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS phones TEXT[] DEFAULT '{}'`,
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS tiktok TEXT`,
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS youtube TEXT`,
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS telegram TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_country TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_city TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_contact_name TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_description TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_location TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_opening_hours TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_rating TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_tiktok TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_youtube TEXT`,
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_telegram TEXT`,
		`CREATE INDEX IF NOT EXISTS idx_bl_country ON business_listings(country) WHERE country IS NOT NULL`,
		`CREATE INDEX IF NOT EXISTS idx_bl_city ON business_listings(city) WHERE city IS NOT NULL`,
		// Niche infrastructure (2026-05-22). The category column is contaminated:
		// ~6K rows are explicit off-niche schema.org types (Hotel, AutoDealer,
		// Dentist, Restaurant, ...) and ~41K are meta-keyword soup (>100 chars).
		// off_niche flag lets the API default-filter to wellness/yoga/fitness
		// results without DELETing data (memory feedback_never_drop_data.md).
		// niche_category is the trigger-classified bucket (yoga, pilates,
		// fitness, wellness, healing, ayurveda, spa, meditation) for the
		// upcoming ?niche= filter. Both columns also wired into the upsert
		// trigger below so new rows get tagged at insert time.
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS off_niche BOOLEAN DEFAULT FALSE`,
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS niche_category TEXT`,
		// completeness_score (0-100): precomputed reenrich-eligibility score, set by
		// trg_normalize_enrichment + a one-time backfill, so the reenrich worker
		// filters on an indexed column instead of a per-candidate correlated EXISTS
		// (the 2026-06-02 eligibility timeout storm). NULL = not yet scored.
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS completeness_score SMALLINT`,
		// idx_bl_niche_active + idx_bl_off_niche_false are created BELOW with
		// CONCURRENTLY (auditor P1 fix): plain CREATE INDEX takes ShareLock on
		// business_listings (779K rows), and with 7 deploy containers racing
		// db.Migrate() against PG_MAX_OPEN_CONNS=2 the lock contention saturates
		// the pool. CONCURRENTLY uses ShareUpdateExclusiveLock instead, which
		// doesn't block concurrent writes. Cannot live inside this batch since
		// CONCURRENTLY is illegal inside a transaction block.
	} {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("db: schema fix 2026-04-27: %w (stmt: %s)", err, stmt)
		}
	}

	// Niche indexes — created CONCURRENTLY outside any tx (CLAUDE.md /
	// CONCURRENTLY is illegal inside a tx block).
	//
	//   idx_bl_niche_active   — serves ?include_off_niche=false + ?niche=X.
	//                            Skips NULL niche_category to keep idx small
	//                            on the long tail of unclassified rows.
	//   idx_bl_off_niche_false — serves the default-listing path (no niche
	//                            filter, just off_niche=false). PK already
	//                            covers ORDER BY bl.id, this partial idx
	//                            backs the WHERE predicate.
	//
	// Failure handling: CONCURRENTLY can race with parallel deploys; one of
	// the 7 containers will succeed, the rest will see IF NOT EXISTS and skip.
	// We log+continue on error rather than failing the entire migration, since
	// the partial idx is an optimization, not a correctness requirement (the
	// planner falls back to PK scan + filter, slower but correct).
	for _, stmt := range []string{
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_niche_active ON business_listings(niche_category) WHERE off_niche IS NOT TRUE AND niche_category IS NOT NULL`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_off_niche_false ON business_listings(id) WHERE off_niche IS NOT TRUE`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			slog.Warn("db: niche index CREATE CONCURRENTLY failed — continuing without it (planner will fall back to PK scan)",
				"stmt", stmt, "error", err)
		}
	}

	// Healing index: makes healZombieQueries fast on the ~116K stuck-processing
	// rows. The reconciler hits this predicate every 60s; without the index it
	// would do a full seqscan on the 3.16M-row queries table.
	//
	// Using CONCURRENTLY (outside any tx) so it doesn't take a ShareLock that
	// would stall writes on the queries table during deploy (same reason as the
	// niche indexes above). CONCURRENTLY cannot run inside a transaction block.
	// Log-and-continue: if it fails (e.g. a previous failed concurrent build
	// left an INVALID index), the migration still succeeds — the planner will
	// fall back to the existing idx_queries_status B-tree, which is slower but
	// correct. The next successful deploy will retry and IF NOT EXISTS will no-op
	// once the valid index exists.
	if _, err := db.Exec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_queries_processing_updated ON queries (updated_at) WHERE status = 'processing'`); err != nil {
		slog.Warn("db: idx_queries_processing_updated CONCURRENTLY failed — planner will use idx_queries_status fallback", "error", err)
	}

	// Backs the reenrich eligibility EXISTS subquery
	// (e.is_acceptable = true OR e.score >= 0.7), which otherwise re-evaluates
	// per candidate row and was a contributor to the 5s statement_timeout that
	// drove the reenrich health-flap (2026-06-01). CONCURRENTLY + log-continue,
	// same pattern as the niche indexes above.
	if _, err := db.Exec(`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_emails_acceptable_score ON emails (is_acceptable, score)`); err != nil {
		slog.Warn("db: idx_emails_acceptable_score CONCURRENTLY failed — reenrich eligibility EXISTS will fall back to PK scan", "error", err)
	}

	// Read-path indexes for business_listings and business_emails.
	//
	//   idx_bl_created_at  — backs ORDER BY created_at DESC on the v2 results
	//                         listing endpoint; without it every paginated read
	//                         on the 779K-row table does a full sort.
	//   idx_bl_updated_at  — backs ORDER BY updated_at DESC for the "recently
	//                         enriched" feed and the reenrich eligibility query.
	//   idx_bl_category    — backs ?category= filter on v2 results; partial
	//                         (category IS NOT NULL) keeps the index small.
	//   idx_be_email_id    — backs the business_emails → emails JOIN on
	//                         email_id; the junction's PK covers (business_id,
	//                         email_id) but not the reverse lookup.
	//
	// All created CONCURRENTLY outside any tx (same reason as the niche indexes
	// above). Log-and-continue: failure here is non-fatal — the planner falls
	// back to PK scans, slower but correct.
	for _, stmt := range []string{
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_created_at ON business_listings (created_at DESC)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_updated_at ON business_listings (updated_at DESC)`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_category ON business_listings (category) WHERE category IS NOT NULL`,
		// Composite for ?category= + the default id_desc sort: serves
		// WHERE category=X ORDER BY bl.id DESC LIMIT N without a separate sort
		// over the (high-cardinality) filtered set, e.g. category=yogaalliance
		// has ~77K rows and timed out the plain category+PK-backward-scan path.
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_category_id ON business_listings (category, id DESC) WHERE category IS NOT NULL`,
		// Partial composite over the DEFAULT off_niche-excluded set: the v2 results
		// count for ?category=X filters `off_niche IS NOT TRUE`, which idx_bl_category
		// can't cover (heap fetch per row → ~12s on the 77K yogaalliance category).
		// This makes the count an index-only scan and the list an index scan.
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_category_offniche ON business_listings (category, id DESC) WHERE off_niche IS NOT TRUE`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_be_email_id ON business_emails (email_id)`,
		// Backs the ?source= filter EXISTS(business_emails.source = $N). Partial on
		// the ~80K non-'enrichment' (directory-crawler) rows only — without it the
		// correlated EXISTS seq-scans 3.9M rows → 57014 timeout (500).
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_be_source ON business_emails (source, business_id) WHERE source <> 'enrichment'`,
		// Backs the reenrich eligibility query: WHERE re_enriched_at IS NULL AND
		// completeness_score < $1. Partial on the (shrinking) un-reenriched set.
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_reenrich_score ON business_listings (completeness_score) WHERE re_enriched_at IS NULL`,
	} {
		if _, err := db.Exec(stmt); err != nil {
			slog.Warn("db: read-path index CREATE CONCURRENTLY failed — continuing without it (planner will fall back to PK scan)",
				"stmt", stmt, "error", err)
		}
	}

	// --- Triggers ---

	// Trigger 1: serp_results INSERT -> create enrichment_job.
	if _, err := db.Exec(`
		CREATE OR REPLACE FUNCTION trg_enqueue_enrichment()
		RETURNS TRIGGER AS $$
		BEGIN
		  INSERT INTO enrichment_jobs (url, url_hash, domain, parent_query_id, source, status)
		  VALUES (NEW.url, NEW.url_hash, NEW.domain, NEW.source_query_id, 'serp_result', 'pending')
		  ON CONFLICT (url_hash) DO NOTHING;
		  RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		return fmt.Errorf("db: create trg_enqueue_enrichment function: %w", err)
	}
	// Bind the trigger only if it is missing. CREATE OR REPLACE FUNCTION above
	// already updates the body lock-free; the previous unconditional
	// DROP TRIGGER … CREATE TRIGGER took an ACCESS EXCLUSIVE lock on serp_results
	// on EVERY boot and was a primary driver of the boot DDL herd. The trigger
	// binding never changes, so create it once and skip thereafter. (If the
	// binding ever needs to change, do it via a one-shot versioned migration.)
	if _, err := db.Exec(`
		DO $$
		BEGIN
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_trigger
		    WHERE tgname = 'trg_serp_results_enqueue' AND NOT tgisinternal
		  ) THEN
		    CREATE TRIGGER trg_serp_results_enqueue
		      AFTER INSERT ON serp_results
		      FOR EACH ROW EXECUTE FUNCTION trg_enqueue_enrichment();
		  END IF;
		END $$;
	`); err != nil {
		return fmt.Errorf("db: create trg_serp_results_enqueue trigger: %w", err)
	}

	// Trigger 2: enrichment_jobs completed -> normalize + queue contact pages.
	if _, err := db.Exec(`
		CREATE OR REPLACE FUNCTION trg_normalize_enrichment()
		RETURNS TRIGGER AS $$
		DECLARE
		  biz_id BIGINT;
		  email_id BIGINT;
		  e TEXT;
		BEGIN
		  IF NEW.status = 'completed' AND (OLD.status IS NULL OR OLD.status != 'completed') THEN
		    -- 1. Upsert business_listings.
		    --    Forward every raw_* the extractor populates. Previously
		    --    description and location were hardcoded NULL here, and
		    --    country/city/contact_name/opening_hours/rating/multi-phone
		    --    had no path at all — extracted then dropped on the floor.
		    --
		    -- Niche classifier inlined here (no Go code, per design):
		    --   • off_niche = TRUE for known off-niche schema.org @type values
		    --     (AutoDealer, Hotel, Restaurant, Dentist, ...) OR meta-keyword
		    --     soup (raw_category length > 100). These are the patterns
		    --     observed polluting business_listings.category — see plan.
		    --   • niche_category bucketed from the union of raw_business_name +
		    --     raw_page_title + raw_description against the same niche keyword
		    --     whitelist used to generate queries in internal/query/wellness.go.
		    --     \m...\M = word boundaries; LOWER() makes the match case-insensitive.
		    INSERT INTO business_listings (domain, url, business_name, category, description,
		        address, location, country, city, contact_name,
		        phone, phones, website, page_title, social_links,
		        opening_hours, rating, tiktok, youtube, telegram, source_query_id,
		        off_niche, niche_category)
		    VALUES (NEW.domain, NEW.url, NEW.raw_business_name, NEW.raw_category, NEW.raw_description,
		        NEW.raw_address, NEW.raw_location, NEW.raw_country, NEW.raw_city, NEW.raw_contact_name,
		        NEW.raw_phones[1], COALESCE(NEW.raw_phones, '{}'), NEW.url, NEW.raw_page_title, NEW.raw_social,
		        NEW.raw_opening_hours, NEW.raw_rating, NEW.raw_tiktok, NEW.raw_youtube, NEW.raw_telegram,
		        NEW.parent_query_id,
		        -- off_niche
		        CASE
		          WHEN NEW.raw_category IS NOT NULL AND LENGTH(NEW.raw_category) > 100 THEN TRUE
		          WHEN NEW.raw_category IN (
		            'AutoDealer','Hotel','Restaurant','Dentist','Physician',
		            'RealEstateAgent','LegalService','HairSalon','BeautySalon',
		            'TravelAgency','LodgingBusiness','GeneralContractor',
		            'RoofingContractor','HomeAndConstructionBusiness',
		            'MedicalClinic','MedicalBusiness','HealthAndBeautyBusiness'
		          ) THEN TRUE
		          ELSE FALSE
		        END,
		        -- niche_category
		        CASE
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\myoga|asana|vinyasa|ashtanga|kundalini|iyengar|hatha|bikram|jivamukti\M' THEN 'yoga'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\mpilates|reformer\M' THEN 'pilates'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\mcrossfit|bootcamp|hiit|barre|spin\M' THEN 'fitness'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\m(gym|fitness)\M' THEN 'fitness'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\mmeditation|mindfulness|breathwork\M' THEN 'meditation'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\mreiki|sound healing|energy healing|healing\M' THEN 'healing'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\mayurved\M' THEN 'ayurveda'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\m(spa|massage|thermal)\M' THEN 'spa'
		          WHEN LOWER(COALESCE(NEW.raw_business_name,'') || ' ' ||
		                     COALESCE(NEW.raw_page_title,'') || ' ' ||
		                     COALESCE(NEW.raw_description,'')) ~ '\m(wellness|holistic)\M' THEN 'wellness'
		          ELSE NULL
		        END
		    )
		    ON CONFLICT (domain) DO UPDATE SET
		        business_name = COALESCE(EXCLUDED.business_name, business_listings.business_name),
		        category      = COALESCE(EXCLUDED.category, business_listings.category),
		        description   = COALESCE(EXCLUDED.description, business_listings.description),
		        address       = COALESCE(EXCLUDED.address, business_listings.address),
		        location      = COALESCE(EXCLUDED.location, business_listings.location),
		        country       = COALESCE(EXCLUDED.country, business_listings.country),
		        city          = COALESCE(EXCLUDED.city, business_listings.city),
		        contact_name  = COALESCE(EXCLUDED.contact_name, business_listings.contact_name),
		        phone         = COALESCE(EXCLUDED.phone, business_listings.phone),
		        -- Phones array: union + dedup. Re-enrichment that captured 1
		        -- phone must NEVER drop the 3 phones found in the prior visit
		        -- (memory feedback_never_drop_data.md / 344K email incident).
		        phones        = ARRAY(SELECT DISTINCT UNNEST(
		                          COALESCE(business_listings.phones, '{}') ||
		                          COALESCE(EXCLUDED.phones, '{}'))),
		        page_title    = COALESCE(EXCLUDED.page_title, business_listings.page_title),
		        social_links  = COALESCE(business_listings.social_links, '{}') || COALESCE(EXCLUDED.social_links, '{}'),
		        opening_hours = COALESCE(EXCLUDED.opening_hours, business_listings.opening_hours),
		        rating        = COALESCE(EXCLUDED.rating, business_listings.rating),
		        tiktok        = COALESCE(EXCLUDED.tiktok, business_listings.tiktok),
		        youtube       = COALESCE(EXCLUDED.youtube, business_listings.youtube),
		        telegram      = COALESCE(EXCLUDED.telegram, business_listings.telegram),
		        -- Niche fields: only promote a TRUE off_niche so a re-enrichment
		        -- of a previously-tagged off-niche row never silently flips back
		        -- to in-niche. niche_category COALESCEs in case re-enrich misses
		        -- the keyword on a shorter page_title.
		        off_niche      = (business_listings.off_niche OR EXCLUDED.off_niche),
		        niche_category = COALESCE(business_listings.niche_category, EXCLUDED.niche_category),
		        updated_at    = NOW();
		    -- 2-step biz_id resolution. RETURNING id INTO biz_id was unreliable
		    -- on the DO UPDATE path when all incoming values were NULL — the
		    -- email junction inserts below would silently skip. Separate SELECT
		    -- guarantees biz_id is populated.
		    SELECT id INTO biz_id FROM business_listings WHERE domain = NEW.domain;

		    -- 2. Upsert emails + junction
		    IF array_length(NEW.raw_emails, 1) > 0 THEN
		      FOREACH e IN ARRAY NEW.raw_emails LOOP
		        INSERT INTO emails (email, domain, local_part)
		        VALUES (e, split_part(e, '@', 2), split_part(e, '@', 1))
		        ON CONFLICT (email) DO NOTHING;

		        SELECT id INTO email_id FROM emails WHERE email = e;

		        IF biz_id IS NOT NULL AND email_id IS NOT NULL THEN
		          INSERT INTO business_emails (business_id, email_id, source)
		          VALUES (biz_id, email_id, 'enrichment')
		          ON CONFLICT DO NOTHING;
		        END IF;
		      END LOOP;
		    END IF;

		    -- 2b. Precompute completeness_score (0-100) so the reenrich worker can
		    -- filter on an indexed column instead of a per-candidate correlated
		    -- EXISTS (the 2026-06-02 eligibility timeout storm). The email-EXISTS
		    -- runs ONCE here per completion, not per scanned candidate. The email
		    -- component reflects validation state AS OF this completion; re_enriched_at
		    -- gates eligibility, so a row is re-scored at most once if validation
		    -- marks an acceptable email later.
		    IF biz_id IS NOT NULL THEN
		      UPDATE business_listings bl SET completeness_score = (
		          CASE WHEN EXISTS(
		            SELECT 1 FROM business_emails be JOIN emails e ON e.id = be.email_id
		            WHERE be.business_id = bl.id AND (e.is_acceptable = true OR e.score >= 0.7)
		          ) THEN 40 ELSE 0 END
		        + CASE WHEN (bl.phone IS NOT NULL AND bl.phone != '')
		              OR (bl.phones IS NOT NULL AND array_length(bl.phones, 1) > 0) THEN 20 ELSE 0 END
		        + CASE WHEN (bl.business_name IS NOT NULL AND bl.business_name != '')
		              AND (bl.category IS NOT NULL AND bl.category != '') THEN 15 ELSE 0 END
		        + CASE WHEN (bl.address IS NOT NULL AND bl.address != '')
		              OR ((bl.city IS NOT NULL AND bl.city != '') AND (bl.country IS NOT NULL AND bl.country != '')) THEN 15 ELSE 0 END
		        + CASE WHEN bl.social_links IS NOT NULL AND bl.social_links != '{}'::jsonb THEN 10 ELSE 0 END
		      ) WHERE bl.id = biz_id;
		    END IF;

		    -- 3. Queue contact pages if serp_result with no emails found
		    IF NEW.source = 'serp_result' AND
		       (NEW.raw_emails IS NULL OR array_length(NEW.raw_emails, 1) = 0) THEN
		      INSERT INTO enrichment_jobs (url, url_hash, domain, parent_query_id, source, status)
		      VALUES
		        ('https://' || NEW.domain || '/contact',    encode(digest('https://' || NEW.domain || '/contact', 'sha256'), 'hex'),    NEW.domain, NEW.parent_query_id, 'contact_page', 'pending'),
		        ('https://' || NEW.domain || '/contact-us', encode(digest('https://' || NEW.domain || '/contact-us', 'sha256'), 'hex'), NEW.domain, NEW.parent_query_id, 'contact_page', 'pending'),
		        ('https://' || NEW.domain || '/about',      encode(digest('https://' || NEW.domain || '/about', 'sha256'), 'hex'),      NEW.domain, NEW.parent_query_id, 'contact_page', 'pending'),
		        ('https://' || NEW.domain || '/about-us',   encode(digest('https://' || NEW.domain || '/about-us', 'sha256'), 'hex'),   NEW.domain, NEW.parent_query_id, 'contact_page', 'pending')
		      ON CONFLICT (url_hash) DO NOTHING;
		    END IF;

		  END IF;
		  RETURN NEW;
		END;
		$$ LANGUAGE plpgsql;
	`); err != nil {
		return fmt.Errorf("db: create trg_normalize_enrichment function: %w", err)
	}
	// CREATE TRIGGER ... WHEN (...) filters at trigger-system level so the
	// function call is skipped entirely for non-completing UPDATEs (heartbeat,
	// locked_by, attempt_count). At peak throughput enrichment_jobs sees
	// thousands of UPDATEs/h that have no normalization work to do; this WHEN
	// clause cuts that overhead to zero.
	// Same idempotent binding as trg_serp_results_enqueue: the function body is
	// kept current by CREATE OR REPLACE FUNCTION above (lock-free); the trigger
	// binding is created once. The previous DROP/CREATE took ACCESS EXCLUSIVE on
	// the 7M-row enrichment_jobs table on every boot. The WHEN filter is part of
	// the binding — if it ever changes, ship a one-shot versioned migration.
	if _, err := db.Exec(`
		DO $$
		BEGIN
		  IF NOT EXISTS (
		    SELECT 1 FROM pg_trigger
		    WHERE tgname = 'trg_enrichment_normalize' AND NOT tgisinternal
		  ) THEN
		    CREATE TRIGGER trg_enrichment_normalize
		      AFTER UPDATE ON enrichment_jobs
		      FOR EACH ROW
		      WHEN (NEW.status = 'completed' AND OLD.status IS DISTINCT FROM 'completed')
		      EXECUTE FUNCTION trg_normalize_enrichment();
		  END IF;
		END $$;
	`); err != nil {
		return fmt.Errorf("db: create trg_enrichment_normalize trigger: %w", err)
	}

	// schema_migrations: lightweight version tracking so one-shot migrations
	// (e.g. Phase 2 backfill) only run once even when the binary restarts.
	// Pre-existing schema is treated as version 0; new versioned migrations
	// register themselves below.
	if _, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS schema_migrations (
			version    TEXT PRIMARY KEY,
			applied_at TIMESTAMPTZ DEFAULT NOW(),
			notes      TEXT
		)
	`); err != nil {
		return fmt.Errorf("db: schema_migrations table: %w", err)
	}

	// Phase 2 backfill 2026-04-27: recover what we can from already-completed
	// enrichment_jobs.raw_* without re-fetching anything. Tracked in
	// schema_migrations so it only runs once (re-running would re-scan the
	// entire enrichment_jobs table for no benefit).
	const phase2Version = "2026_04_27_phase2_backfill"
	var phase2Done bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, phase2Version,
	).Scan(&phase2Done); err != nil {
		return fmt.Errorf("db: phase 2 version check: %w", err)
	}
	if !phase2Done {
		// State codes that are NOT countries — must be excluded from the
		// 2-letter regex country branch. US state codes like "CA" "NY" "TX"
		// would otherwise be mis-stored as country (Canada, NY-non-existent,
		// Turks-and-Caicos). Same for AU/CA/BR/DE state codes. Lower-case
		// for case-insensitive comparison.
		stateCodeBlocklist := `'al','ak','az','ar','ca','co','ct','de','fl','ga','hi','id','il','in','ia','ks','ky','la','me','md','ma','mi','mn','ms','mo','mt','ne','nv','nh','nj','nm','ny','nc','nd','oh','ok','or','pa','ri','sc','sd','tn','tx','ut','vt','va','wa','wv','wi','wy',` + // US states
			`'nsw','vic','qld','wa','sa','tas','act','nt',` + // Australia (already lowercased)
			`'on','qc','bc','mb','nb','nl','ns','pe','sk','yt','nu',` + // Canada provinces
			`'sp','rj','mg','ba','rs','pr','sc','pe','ce','pa','go'` // Brazil

		backfillStmts := []struct {
			name string
			sql  string
		}{
			{
				"promote_raw_phones",
				`UPDATE business_listings bl
				 SET phones = ej.raw_phones
				 FROM enrichment_jobs ej
				 WHERE ej.domain = bl.domain
				   AND ej.status = 'completed'
				   AND COALESCE(array_length(ej.raw_phones, 1), 0) > 0
				   AND COALESCE(array_length(bl.phones, 1), 0) = 0`,
			},
			{
				// Country: case-insensitive, accepts ISO-2 OR known country
				// names. State codes excluded via NOT IN. CTE prevents
				// regexp_split_to_array from running twice (2x perf at scale).
				"recover_country",
				`WITH parsed AS (
				   SELECT ej.domain,
				          (regexp_match(ej.raw_address, '(?i)([A-Z]{2,3}|United States|United Kingdom|Indonesia|Australia|Canada|Germany|France|Singapore|Malaysia|Thailand|Japan|Brazil|Mexico|Spain|Italy|Netherlands|Belgium|Sweden|Norway|Denmark|Finland|Poland|Turkey|India|China|Korea|Vietnam|Philippines|New Zealand)\s*$'))[1] AS code
				   FROM enrichment_jobs ej
				   WHERE ej.status = 'completed' AND ej.raw_address IS NOT NULL
				 )
				 UPDATE business_listings bl
				 SET country = UPPER(parsed.code)
				 FROM parsed
				 WHERE parsed.domain = bl.domain
				   AND parsed.code IS NOT NULL
				   AND LOWER(parsed.code) NOT IN (` + stateCodeBlocklist + `)
				   AND (bl.country IS NULL OR bl.country = '')`,
			},
			{
				"recover_city",
				`WITH parts AS (
				   SELECT ej.domain, regexp_split_to_array(ej.raw_address, ',') AS segs
				   FROM enrichment_jobs ej
				   WHERE ej.status = 'completed' AND ej.raw_address IS NOT NULL
				 )
				 UPDATE business_listings bl
				 SET city = TRIM(BOTH ' ,' FROM parts.segs[array_length(parts.segs, 1) - 1])
				 FROM parts
				 WHERE parts.domain = bl.domain
				   AND array_length(parts.segs, 1) >= 3
				   AND (bl.city IS NULL OR bl.city = '')`,
			},
			{
				// Drop tiktok/youtube/telegram keys from social_links JSONB
				// after promoting them to flat columns — single source of
				// truth (per architect Finding 5).
				"normalize_social_links",
				`UPDATE business_listings
				 SET social_links = social_links - 'tiktok' - 'youtube' - 'telegram'
				 WHERE social_links ?| array['tiktok','youtube','telegram']`,
			},
		}

		for _, b := range backfillStmts {
			res, err := db.Exec(b.sql)
			if err != nil {
				slog.Warn("db: phase 2 backfill failed", "step", b.name, "error", err)
				continue
			}
			n, _ := res.RowsAffected()
			slog.Info("db: phase 2 backfill", "step", b.name, "rows_updated", n)
		}

		if _, err := db.Exec(
			`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2)
			 ON CONFLICT (version) DO NOTHING`,
			phase2Version,
			"backfill country/city/phones from raw_address; normalize social_links JSONB",
		); err != nil {
			slog.Warn("db: phase 2 version record failed", "error", err)
		}
	}

	// Phase 4 Tier 1 — normalize existing country values that pre-date the
	// ISO 3166-1 alpha-2 enforcement. Old rows might have "USA", "United
	// States", "u.s.a.", "indonesia" as raw text. Fold them to canonical
	// alpha-2 so country filter queries match across all rows.
	const tier1Version = "2026_04_27_tier1_country_normalize"
	var tier1Done bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, tier1Version,
	).Scan(&tier1Done); err == nil && !tier1Done {
		// Map common variants in a single UPDATE. CASE expression hits each
		// row at most once. Skip rows already in canonical 2-char form.
		res, err := db.Exec(`
			UPDATE business_listings SET country = CASE LOWER(TRIM(country))
				WHEN 'usa' THEN 'US' WHEN 'u.s.a.' THEN 'US' WHEN 'u.s.' THEN 'US'
				WHEN 'united states' THEN 'US' WHEN 'united states of america' THEN 'US' WHEN 'america' THEN 'US'
				WHEN 'uk' THEN 'GB' WHEN 'united kingdom' THEN 'GB' WHEN 'great britain' THEN 'GB'
				WHEN 'england' THEN 'GB' WHEN 'scotland' THEN 'GB' WHEN 'wales' THEN 'GB'
				WHEN 'indonesia' THEN 'ID' WHEN 'republic of indonesia' THEN 'ID'
				WHEN 'australia' THEN 'AU' WHEN 'canada' THEN 'CA'
				WHEN 'germany' THEN 'DE' WHEN 'deutschland' THEN 'DE'
				WHEN 'france' THEN 'FR' WHEN 'spain' THEN 'ES' WHEN 'españa' THEN 'ES'
				WHEN 'italy' THEN 'IT' WHEN 'italia' THEN 'IT'
				WHEN 'netherlands' THEN 'NL' WHEN 'nederland' THEN 'NL' WHEN 'holland' THEN 'NL'
				WHEN 'singapore' THEN 'SG' WHEN 'malaysia' THEN 'MY' WHEN 'thailand' THEN 'TH'
				WHEN 'japan' THEN 'JP' WHEN 'korea' THEN 'KR' WHEN 'south korea' THEN 'KR'
				WHEN 'china' THEN 'CN' WHEN 'vietnam' THEN 'VN' WHEN 'viet nam' THEN 'VN'
				WHEN 'philippines' THEN 'PH' WHEN 'india' THEN 'IN'
				WHEN 'brazil' THEN 'BR' WHEN 'brasil' THEN 'BR'
				WHEN 'mexico' THEN 'MX' WHEN 'méxico' THEN 'MX'
				WHEN 'new zealand' THEN 'NZ' WHEN 'switzerland' THEN 'CH'
				WHEN 'belgium' THEN 'BE' WHEN 'sweden' THEN 'SE'
				WHEN 'norway' THEN 'NO' WHEN 'denmark' THEN 'DK' WHEN 'finland' THEN 'FI'
				WHEN 'poland' THEN 'PL' WHEN 'turkey' THEN 'TR' WHEN 'türkiye' THEN 'TR'
				WHEN 'united arab emirates' THEN 'AE' WHEN 'uae' THEN 'AE'
				WHEN 'saudi arabia' THEN 'SA' WHEN 'south africa' THEN 'ZA'
				ELSE country
			END
			WHERE country IS NOT NULL
			  AND length(country) > 2
		`)
		if err != nil {
			slog.Warn("db: tier 1 country normalize failed", "error", err)
		} else {
			n, _ := res.RowsAffected()
			slog.Info("db: tier 1 country normalize", "rows_updated", n)
			db.Exec(
				`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2)
				 ON CONFLICT (version) DO NOTHING`,
				tier1Version,
				"normalize existing country values to ISO 3166-1 alpha-2",
			)
		}
	}

	// re_enriched_at: tracks which business_listings rows have been processed by
	// the autonomous reenrich worker. NULL = eligible for re-enrichment.
	// Manual trigger for specific domains:
	//   UPDATE business_listings SET re_enriched_at = NULL WHERE domain IN ('example.com', ...)
	const reenrichColVersion = "2026_04_27_reenrich_col"
	var reenrichColDone bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, reenrichColVersion,
	).Scan(&reenrichColDone); err == nil && !reenrichColDone {
		stmts := []string{
			`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS re_enriched_at TIMESTAMPTZ`,
			`CREATE INDEX IF NOT EXISTS idx_bl_re_enriched_at ON business_listings(re_enriched_at) WHERE re_enriched_at IS NULL`,
		}
		migOK := true
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				slog.Warn("db: reenrich col migration failed", "error", err)
				migOK = false
				break
			}
		}
		if migOK {
			db.Exec(`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
				reenrichColVersion, "add re_enriched_at to business_listings for autonomous reenrich worker")
			slog.Info("db: reenrich col migration applied")
		}
	}

	// re_enrich_locked_at: claim sentinel for multi-worker coordination.
	// Set to NOW() when a worker claims a row via FOR UPDATE SKIP LOCKED.
	// Cleared on completion (success or release). Stale claims (>15 min)
	// are auto-recovered by the eligibility query — no janitor needed.
	const reenrichLockVersion = "2026_04_27_reenrich_lock"
	var reenrichLockDone bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, reenrichLockVersion,
	).Scan(&reenrichLockDone); err == nil && !reenrichLockDone {
		stmts := []string{
			`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS re_enrich_locked_at TIMESTAMPTZ`,
			`CREATE INDEX IF NOT EXISTS idx_bl_re_enrich_locked_at ON business_listings(re_enrich_locked_at) WHERE re_enriched_at IS NULL`,
		}
		migOK := true
		for _, s := range stmts {
			if _, err := db.Exec(s); err != nil {
				slog.Warn("db: reenrich lock migration failed", "error", err)
				migOK = false
				break
			}
		}
		if migOK {
			db.Exec(`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
				reenrichLockVersion, "add re_enrich_locked_at for multi-worker FOR UPDATE SKIP LOCKED claim")
			slog.Info("db: reenrich lock migration applied")
		}
	}

	// -------------------------------------------------------------------------
	// 2026-05-12: Country regex hardening (Deliverable 1).
	//
	// Background: the original phase2Version recover_country step used
	// `[A-Z]{2,3}` as the first alternative in the regex, which captured the
	// last 2-3 uppercase characters of ANY string (e.g. "LS7 1AB" → "AB",
	// "Den Haag" → "AAG", "Schönebach" → "ACH"). The stateCodeBlocklist only
	// filtered US/AU/CA/BR state codes, so random suffixes like "AAG", "ACH",
	// "ADO", "ADS" passed through.
	//
	// Fix: remove [A-Z]{2,3} entirely. Match only known full country names
	// (same whitelist as before), then map them to ISO alpha-2 via CASE.
	// The WHERE guard `(bl.country IS NULL OR bl.country = '')` is retained so
	// this step only fills blanks — it will not overwrite any existing value.
	//
	// This step is intentionally idempotent: running it again on rows that
	// already have a correct country is a no-op due to the WHERE guard.
	// -------------------------------------------------------------------------
	const countryRegexHardenVersion = "2026_05_12_country_regex_hardening"
	var countryRegexHardenDone bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, countryRegexHardenVersion,
	).Scan(&countryRegexHardenDone); err != nil {
		return fmt.Errorf("db: country regex harden version check: %w", err)
	}
	if !countryRegexHardenDone {
		// Match only spelled-out country names at end of address string.
		// [A-Z]{2,3} removed entirely — it was too greedy and caused 8,177
		// garbage rows (COM, AND, ACH, AAG, etc.) in production.
		// The CASE below maps each matched name to its ISO 3166-1 alpha-2 code.
		countryRegexHardenSQL := `
		WITH parsed AS (
		  SELECT ej.domain,
		         (regexp_match(ej.raw_address,
		           '(?i)(United States|United Kingdom|Indonesia|Australia|Canada|Germany|France|Singapore|Malaysia|Thailand|Japan|Brazil|Mexico|Spain|Italy|Netherlands|Belgium|Sweden|Norway|Denmark|Finland|Poland|Turkey|India|China|Korea|Vietnam|Philippines|New Zealand|Portugal|Ireland|Croatia|Austria|Chile|Colombia|Hungary|Romania|Greece|Taiwan|Morocco|Argentina|Egypt|Myanmar|Costa Rica|Panama|Kenya|Bahrain|Qatar|Nepal|Nigeria|Sri Lanka|Cambodia|Switzerland|South Africa|United Arab Emirates|Saudi Arabia|South Korea|New Zealand|Czech Republic|Hong Kong)\s*$'))[1]
		         AS matched_name
		  FROM enrichment_jobs ej
		  WHERE ej.status = 'completed' AND ej.raw_address IS NOT NULL
		)
		UPDATE business_listings bl
		SET country = CASE LOWER(parsed.matched_name)
		  WHEN 'united states'           THEN 'US'
		  WHEN 'united kingdom'          THEN 'GB'
		  WHEN 'indonesia'               THEN 'ID'
		  WHEN 'australia'               THEN 'AU'
		  WHEN 'canada'                  THEN 'CA'
		  WHEN 'germany'                 THEN 'DE'
		  WHEN 'france'                  THEN 'FR'
		  WHEN 'singapore'               THEN 'SG'
		  WHEN 'malaysia'                THEN 'MY'
		  WHEN 'thailand'                THEN 'TH'
		  WHEN 'japan'                   THEN 'JP'
		  WHEN 'brazil'                  THEN 'BR'
		  WHEN 'mexico'                  THEN 'MX'
		  WHEN 'spain'                   THEN 'ES'
		  WHEN 'italy'                   THEN 'IT'
		  WHEN 'netherlands'             THEN 'NL'
		  WHEN 'belgium'                 THEN 'BE'
		  WHEN 'sweden'                  THEN 'SE'
		  WHEN 'norway'                  THEN 'NO'
		  WHEN 'denmark'                 THEN 'DK'
		  WHEN 'finland'                 THEN 'FI'
		  WHEN 'poland'                  THEN 'PL'
		  WHEN 'turkey'                  THEN 'TR'
		  WHEN 'india'                   THEN 'IN'
		  WHEN 'china'                   THEN 'CN'
		  WHEN 'korea'                   THEN 'KR'
		  WHEN 'south korea'             THEN 'KR'
		  WHEN 'vietnam'                 THEN 'VN'
		  WHEN 'philippines'             THEN 'PH'
		  WHEN 'new zealand'             THEN 'NZ'
		  WHEN 'portugal'                THEN 'PT'
		  WHEN 'ireland'                 THEN 'IE'
		  WHEN 'croatia'                 THEN 'HR'
		  WHEN 'austria'                 THEN 'AT'
		  WHEN 'chile'                   THEN 'CL'
		  WHEN 'colombia'                THEN 'CO'
		  WHEN 'hungary'                 THEN 'HU'
		  WHEN 'romania'                 THEN 'RO'
		  WHEN 'greece'                  THEN 'GR'
		  WHEN 'taiwan'                  THEN 'TW'
		  WHEN 'morocco'                 THEN 'MA'
		  WHEN 'argentina'               THEN 'AR'
		  WHEN 'egypt'                   THEN 'EG'
		  WHEN 'myanmar'                 THEN 'MM'
		  WHEN 'costa rica'              THEN 'CR'
		  WHEN 'panama'                  THEN 'PA'
		  WHEN 'kenya'                   THEN 'KE'
		  WHEN 'bahrain'                 THEN 'BH'
		  WHEN 'qatar'                   THEN 'QA'
		  WHEN 'nepal'                   THEN 'NP'
		  WHEN 'nigeria'                 THEN 'NG'
		  WHEN 'sri lanka'               THEN 'LK'
		  WHEN 'cambodia'                THEN 'KH'
		  WHEN 'switzerland'             THEN 'CH'
		  WHEN 'south africa'            THEN 'ZA'
		  WHEN 'united arab emirates'    THEN 'AE'
		  WHEN 'saudi arabia'            THEN 'SA'
		  WHEN 'czech republic'          THEN 'CZ'
		  WHEN 'hong kong'               THEN 'HK'
		  ELSE NULL
		END
		FROM parsed
		WHERE parsed.domain = bl.domain
		  AND parsed.matched_name IS NOT NULL
		  AND (bl.country IS NULL OR bl.country = '')`

		res, err := db.Exec(countryRegexHardenSQL)
		if err != nil {
			slog.Warn("db: country regex hardening failed", "error", err)
		} else {
			n, _ := res.RowsAffected()
			slog.Info("db: country regex hardening applied", "rows_updated", n)
		}

		if _, err := db.Exec(
			`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
			countryRegexHardenVersion,
			"fix recover_country regex: remove [A-Z]{2,3} greedy alternative, match known country names only",
		); err != nil {
			slog.Warn("db: country regex harden version record failed", "error", err)
		}
	}

	// -------------------------------------------------------------------------
	// 2026-05-12: Country garbage purge (Deliverable 2).
	//
	// Pre-condition: ~8,177 rows in business_listings have garbage country
	// values (COM, AND, ACH, AAG, ONS, etc.) from the buggy [A-Z]{2,3}
	// regex in the original phase2Version recover_country step.
	// Additionally ~2,624 rows have spelled-out country names (Portugal,
	// Ireland, etc.) that the tier1 normalize step missed.
	//
	// Steps:
	//   A. BACKUP: CREATE TABLE ... AS SELECT — persists garbage values
	//              before any mutation. Rollback: UPDATE bl SET country =
	//              b.country FROM backup b WHERE bl.id = b.id.
	//   B. CONVERT: salvage the spelled-out country names (Portugal → 'PT',
	//               Ireland → 'IE', etc.) — these are real data, not garbage.
	//   C. PURGE:   set country = NULL for remaining non-ISO-alpha2 values
	//               (the 3-4 char uppercase garbage: COM, AND, ACH, etc.).
	//   D. VERIFY:  log post-cleanup counts.
	//
	// Dry-run gate: set COUNTRY_CLEANUP_DRY_RUN=true to log affected counts
	// without applying any UPDATE/CREATE. Default is OFF (cleanup runs).
	// Per memory rule feedback_gated_flag_for_risky_changes.md — gate risky
	// changes behind an off-by-default flag for first production run.
	//
	// Statement timeout: 30s per batch. The UPDATE touches at most ~8,177
	// rows on a 483K-row table; with an index on country this is fast, but
	// we cap it to avoid unexpected lock escalation.
	// -------------------------------------------------------------------------
	const countryGarbagePurgeVersion = "2026_05_12_country_garbage_purge"
	var countryGarbagePurgeDone bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, countryGarbagePurgeVersion,
	).Scan(&countryGarbagePurgeDone); err != nil {
		return fmt.Errorf("db: country garbage purge version check: %w", err)
	}
	if !countryGarbagePurgeDone {
		// plausibleAlpha2 is the inline SQL literal of the canonical ISO
		// 3166-1 alpha-2 set. Kept as a SQL constant here because the
		// iso3166Alpha2 Go map lives in internal/scraper (different package)
		// and cannot be imported from internal/db without a circular dep.
		// Generated from internal/scraper/contact.go:iso3166Alpha2.
		// Rollback SQL (documented for ops):
		//   UPDATE business_listings bl
		//   SET country = b.country, updated_at = b.updated_at
		//   FROM business_listings_country_backup_20260512 b
		//   WHERE bl.id = b.id;
		const plausibleAlpha2 = `'AD','AE','AF','AG','AI','AL','AM','AO',
		'AQ','AR','AS','AT','AU','AW','AX','AZ',
		'BA','BB','BD','BE','BF','BG','BH','BI',
		'BJ','BL','BM','BN','BO','BQ','BR','BS',
		'BT','BV','BW','BY','BZ','CA','CC','CD',
		'CF','CG','CH','CI','CK','CL','CM','CN',
		'CO','CR','CU','CV','CW','CX','CY','CZ',
		'DE','DJ','DK','DM','DO','DZ','EC','EE',
		'EG','EH','ER','ES','ET','FI','FJ','FK',
		'FM','FO','FR','GA','GB','GD','GE','GF',
		'GG','GH','GI','GL','GM','GN','GP','GQ',
		'GR','GS','GT','GU','GW','GY','HK','HM',
		'HN','HR','HT','HU','ID','IE','IL','IM',
		'IN','IO','IQ','IR','IS','IT','JE','JM',
		'JO','JP','KE','KG','KH','KI','KM','KN',
		'KP','KR','KW','KY','KZ','LA','LB','LC',
		'LI','LK','LR','LS','LT','LU','LV','LY',
		'MA','MC','MD','ME','MF','MG','MH','MK',
		'ML','MM','MN','MO','MP','MQ','MR','MS',
		'MT','MU','MV','MW','MX','MY','MZ','NA',
		'NC','NE','NF','NG','NI','NL','NO','NP',
		'NR','NU','NZ','OM','PA','PE','PF','PG',
		'PH','PK','PL','PM','PN','PR','PS','PT',
		'PW','PY','QA','RE','RO','RS','RU','RW',
		'SA','SB','SC','SD','SE','SG','SH','SI',
		'SJ','SK','SL','SM','SN','SO','SR','SS',
		'ST','SV','SX','SY','SZ','TC','TD','TF',
		'TG','TH','TJ','TK','TL','TM','TN','TO',
		'TR','TT','TV','TW','TZ','UA','UG','UM',
		'US','UY','UZ','VA','VC','VE','VG','VI',
		'VN','VU','WF','WS','YE','YT','ZA','ZM',
		'ZW'`

		dryRun := os.Getenv("COUNTRY_CLEANUP_DRY_RUN") == "true"
		if dryRun {
			slog.Info("db: country garbage purge DRY-RUN mode — counting affected rows, no mutation applied")

			var garbageCount int
			_ = db.QueryRow(`
				SELECT COUNT(*) FROM business_listings
				WHERE country IS NOT NULL
				  AND country != ''
				  AND country NOT IN (` + plausibleAlpha2 + `)`,
			).Scan(&garbageCount)

			var nameCount int
			_ = db.QueryRow(`
				SELECT COUNT(*) FROM business_listings
				WHERE country IS NOT NULL AND length(country) > 4`,
			).Scan(&nameCount)

			slog.Info("db: country garbage purge dry-run counts",
				"garbage_to_null", garbageCount-nameCount,
				"names_to_convert", nameCount,
				"total_affected", garbageCount,
			)
			// Do NOT record version — dry-run should re-run on next deploy
			// when operator removes COUNTRY_CLEANUP_DRY_RUN=true.
			return nil
		}

		// STEP A — BACKUP: capture all non-plausible country values before
		// any mutation. Backup table is append-safe; IF NOT EXISTS prevents
		// failure on re-run if backup already exists from a prior attempt.
		//
		// Integrity gate: count live garbage rows BEFORE creating the backup,
		// then verify backup row count matches. If backup is empty or the count
		// diverges by more than 10%, abort STEPS B and C to prevent data loss
		// on a silent CREATE failure (e.g. table already existed with old data).
		var liveGarbageCount int
		if err := db.QueryRow(`
			SELECT COUNT(*) FROM business_listings
			WHERE country IS NOT NULL
			  AND country != ''
			  AND country NOT IN (` + plausibleAlpha2 + `)`).Scan(&liveGarbageCount); err != nil {
			slog.Warn("db: country garbage purge pre-backup count failed — aborting", "error", err)
			return nil
		}
		slog.Info("db: country garbage purge pre-backup count", "live_garbage_rows", liveGarbageCount)

		_, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS business_listings_country_backup_20260512 AS
			SELECT id, country, updated_at
			FROM business_listings
			WHERE country IS NOT NULL
			  AND country != ''
			  AND country NOT IN (` + plausibleAlpha2 + `)`)
		if err != nil {
			slog.Warn("db: country garbage purge backup failed — aborting purge for safety", "error", err)
			return nil // abort this step; do not purge without backup
		}
		var backupCount int
		_ = db.QueryRow(`SELECT COUNT(*) FROM business_listings_country_backup_20260512`).Scan(&backupCount)
		slog.Info("db: country garbage purge backup created", "backup_rows", backupCount, "expected_rows", liveGarbageCount)

		// Integrity gate: abort if backup is empty (total failure) or count
		// diverges more than 10% from live count (partial failure / stale table).
		// A 10% tolerance covers rows legitimately converted between the pre-count
		// query and the CREATE TABLE AS SELECT (concurrent pipeline activity).
		if backupCount == 0 && liveGarbageCount > 0 {
			slog.Warn("db: country garbage purge backup integrity FAILED — backup is empty, aborting purge",
				"live_garbage_rows", liveGarbageCount)
			return nil
		}
		if liveGarbageCount > 0 {
			divergePct := float64(liveGarbageCount-backupCount) / float64(liveGarbageCount) * 100
			if divergePct < 0 {
				divergePct = -divergePct
			}
			if divergePct > 10 {
				slog.Warn("db: country garbage purge backup integrity FAILED — count diverges, aborting purge",
					"live_garbage_rows", liveGarbageCount,
					"backup_rows", backupCount,
					"diverge_pct", divergePct)
				return nil
			}
		}
		slog.Info("db: country garbage purge backup integrity verified — proceeding with STEPS B and C")

		// STEP B — CONVERT: salvage all recognizable country names and common
		// aliases not in the ISO alpha-2 set. Covers:
		//   • Spelled-out names (>4 chars): Portugal, Ireland, Croatia, etc.
		//   • Short country names (3-4 chars): Peru, Cuba, Oman, Laos
		//   • Common aliases (2 chars): UK → GB
		// The CASE uses ELSE country (not NULL) so unrecognized values pass
		// through unchanged — they are caught by STEP C.
		// Uses an explicit transaction so SET LOCAL statement_timeout applies
		// only to this UPDATE (~2,812 rows after adding UK+Peru).
		if txB, txErr := db.Begin(); txErr != nil {
			slog.Warn("db: country garbage purge convert tx begin failed", "error", txErr)
		} else {
			txB.Exec(`SET LOCAL statement_timeout = '30000'`) //nolint:errcheck — advisory
			_, convErr := txB.Exec(`
				UPDATE business_listings SET
				  country = CASE LOWER(TRIM(country))
				    -- 2-char aliases not in ISO alpha-2
				    WHEN 'uk'              THEN 'GB'
				    -- Short country names (3-4 chars)
				    WHEN 'peru'            THEN 'PE'
				    WHEN 'cuba'            THEN 'CU'
				    WHEN 'iran'            THEN 'IR'
				    WHEN 'iraq'            THEN 'IQ'
				    WHEN 'oman'            THEN 'OM'
				    WHEN 'laos'            THEN 'LA'
				    WHEN 'fiji'            THEN 'FJ'
				    WHEN 'mali'            THEN 'ML'
				    WHEN 'chad'            THEN 'TD'
				    WHEN 'togo'            THEN 'TG'
				    WHEN 'guam'            THEN 'GU'
				    -- Spelled-out country names (>4 chars)
				    WHEN 'portugal'        THEN 'PT'
				    WHEN 'ireland'         THEN 'IE'
				    WHEN 'croatia'         THEN 'HR'
				    WHEN 'austria'         THEN 'AT'
				    WHEN 'chile'           THEN 'CL'
				    WHEN 'colombia'        THEN 'CO'
				    WHEN 'hungary'         THEN 'HU'
				    WHEN 'romania'         THEN 'RO'
				    WHEN 'greece'          THEN 'GR'
				    WHEN 'taiwan'          THEN 'TW'
				    WHEN 'morocco'         THEN 'MA'
				    WHEN 'argentina'       THEN 'AR'
				    WHEN 'egypt'           THEN 'EG'
				    WHEN 'czech republic'  THEN 'CZ'
				    WHEN 'myanmar'         THEN 'MM'
				    WHEN 'costa rica'      THEN 'CR'
				    WHEN 'panama'          THEN 'PA'
				    WHEN 'kenya'           THEN 'KE'
				    WHEN 'bahrain'         THEN 'BH'
				    WHEN 'qatar'           THEN 'QA'
				    WHEN 'nepal'           THEN 'NP'
				    WHEN 'nigeria'         THEN 'NG'
				    WHEN 'sri lanka'       THEN 'LK'
				    WHEN 'cambodia'        THEN 'KH'
				    WHEN 'hong kong'       THEN 'HK'
				    WHEN 'switzerland'     THEN 'CH'
				    WHEN 'south africa'    THEN 'ZA'
				    WHEN 'united arab emirates' THEN 'AE'
				    WHEN 'saudi arabia'    THEN 'SA'
				    WHEN 'united states'   THEN 'US'
				    WHEN 'united kingdom'  THEN 'GB'
				    WHEN 'indonesia'       THEN 'ID'
				    WHEN 'australia'       THEN 'AU'
				    WHEN 'canada'          THEN 'CA'
				    WHEN 'germany'         THEN 'DE'
				    WHEN 'france'          THEN 'FR'
				    WHEN 'singapore'       THEN 'SG'
				    WHEN 'malaysia'        THEN 'MY'
				    WHEN 'thailand'        THEN 'TH'
				    WHEN 'brazil'          THEN 'BR'
				    WHEN 'mexico'          THEN 'MX'
				    WHEN 'spain'           THEN 'ES'
				    WHEN 'italy'           THEN 'IT'
				    WHEN 'netherlands'     THEN 'NL'
				    WHEN 'belgium'         THEN 'BE'
				    WHEN 'sweden'          THEN 'SE'
				    WHEN 'norway'          THEN 'NO'
				    WHEN 'denmark'         THEN 'DK'
				    WHEN 'finland'         THEN 'FI'
				    WHEN 'poland'          THEN 'PL'
				    WHEN 'turkey'          THEN 'TR'
				    WHEN 'india'           THEN 'IN'
				    WHEN 'china'           THEN 'CN'
				    WHEN 'korea'           THEN 'KR'
				    WHEN 'south korea'     THEN 'KR'
				    WHEN 'vietnam'         THEN 'VN'
				    WHEN 'philippines'     THEN 'PH'
				    WHEN 'new zealand'     THEN 'NZ'
				    WHEN 'japan'           THEN 'JP'
				    ELSE country
				  END,
				  updated_at = NOW()
				WHERE country IS NOT NULL
				  AND country != ''
				  AND country NOT IN (` + plausibleAlpha2 + `)`)
			if convErr != nil {
				txB.Rollback() //nolint:errcheck
				slog.Warn("db: country garbage purge convert step failed", "error", convErr)
				// Non-fatal: purge step C still handles remaining garbage.
			} else {
				txB.Commit() //nolint:errcheck
				slog.Info("db: country garbage purge convert step applied (names → ISO codes)")
			}
		}

		// STEP C — PURGE: set country = NULL for all remaining non-ISO-alpha2
		// values (garbage: COM, AND, ACH, AAG, ONS, etc.).
		// Uses explicit transaction so SET LOCAL statement_timeout is scoped.
		// Lock class: ROW EXCLUSIVE — not ACCESS EXCLUSIVE; concurrent SELECTs
		// are unaffected. At most ~5,553 rows (3-4 char uppercase garbage).
		var purgedRows int64
		if txC, txErr := db.Begin(); txErr != nil {
			slog.Warn("db: country garbage purge step C tx begin failed", "error", txErr)
		} else {
			txC.Exec(`SET LOCAL statement_timeout = '30000'`) //nolint:errcheck — advisory
			res, purgeErr := txC.Exec(`
				UPDATE business_listings
				SET country = NULL, updated_at = NOW()
				WHERE country IS NOT NULL
				  AND country != ''
				  AND country NOT IN (` + plausibleAlpha2 + `)`)
			if purgeErr != nil {
				txC.Rollback() //nolint:errcheck
				slog.Warn("db: country garbage purge step C failed", "error", purgeErr)
			} else {
				txC.Commit() //nolint:errcheck
				purgedRows, _ = res.RowsAffected()
				slog.Info("db: country garbage purge applied", "rows_nulled", purgedRows)
			}
		}

		// STEP D — VERIFY: post-cleanup count of remaining garbage.
		var remainingGarbage int
		_ = db.QueryRow(`
			SELECT COUNT(*) FROM business_listings
			WHERE country IS NOT NULL
			  AND country != ''
			  AND country NOT IN (` + plausibleAlpha2 + `)`,
		).Scan(&remainingGarbage)
		if remainingGarbage > 0 {
			slog.Warn("db: country garbage purge: residual garbage detected after purge",
				"remaining_garbage_rows", remainingGarbage)
		} else {
			slog.Info("db: country garbage purge: verified clean — zero garbage rows remain")
		}

		if _, err := db.Exec(
			`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
			countryGarbagePurgeVersion,
			"backup garbage country rows, convert country names to ISO, purge remaining garbage",
		); err != nil {
			slog.Warn("db: country garbage purge version record failed", "error", err)
		}
	}

	// -------------------------------------------------------------------------
	// One-shot migration 2026-05-22: Off-niche backfill cleanup.
	//
	// The trigger above started tagging NEW rows with off_niche + niche_category
	// at insert time. Existing 779K rows in business_listings still carry the
	// pollution from before the trigger landed: ~6K rows with explicit off-niche
	// schema.org @type values (Hotel=1838, MedicalBusiness=787, MedicalClinic=730,
	// Restaurant=666, Store=489, AutoDealer=332, Physician=321, BeautySalon=310,
	// LegalService=269, TravelAgency=240, HairSalon=239, LodgingBusiness=236,
	// Dentist=193, RealEstateAgent=174, HomeAndConstructionBusiness=118,
	// GeneralContractor=26, RoofingContractor=12, HealthAndBeautyBusiness=804)
	// plus ~41K rows where category is >100 chars (meta-keyword soup from
	// <meta name="keywords"> instead of schema.org @type).
	//
	// Steps mirror the country garbage purge (versioned, backup-first), but
	// without an env-var dry-run flag — the backup + integrity gate below
	// (count-divergence check + abort-on-empty-backup) provides equivalent
	// safety without operational toggles. Counts are logged BEFORE mutation
	// so the operator can verify in the deploy log without code changes.
	//   A. PRE-COUNT — log live affected row count (visible in deploy log).
	//   B. BACKUP — copy id+category+off_niche of all rows about to mutate.
	//      Integrity gate: backup must be non-empty AND within 10% of live count.
	//   C. FLAG — UPDATE off_niche=TRUE for rows matching either rule.
	//   D. CLASSIFY — forward-fill niche_category for off_niche=FALSE rows
	//                 whose category text matches the niche regex used by the
	//                 trigger. No mutation for rows the regex doesn't match.
	//   E. VERIFY — log post-cleanup counts.
	//
	// Rollback (documented for ops):
	//   UPDATE business_listings bl
	//   SET off_niche = b.off_niche,
	//       niche_category = b.niche_category,
	//       updated_at = b.updated_at
	//   FROM business_listings_offniche_backup_20260522 b
	//   WHERE bl.id = b.id;
	//   DELETE FROM schema_migrations WHERE version = '2026_05_22_off_niche_backfill';
	// -------------------------------------------------------------------------
	const offNicheCleanupVersion = "2026_05_22_off_niche_backfill"
	var offNicheCleanupDone bool
	if err := db.QueryRow(
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`, offNicheCleanupVersion,
	).Scan(&offNicheCleanupDone); err != nil {
		return fmt.Errorf("db: off-niche cleanup version check: %w", err)
	}
	if !offNicheCleanupDone {
		// Off-niche schema.org @type list — kept in sync with the trigger above.
		const offNicheTypes = `'AutoDealer','Hotel','Restaurant','Dentist','Physician',
		'RealEstateAgent','LegalService','HairSalon','BeautySalon',
		'TravelAgency','LodgingBusiness','GeneralContractor',
		'RoofingContractor','HomeAndConstructionBusiness',
		'MedicalClinic','MedicalBusiness','HealthAndBeautyBusiness'`

		// STEP A — PRE-COUNT: log counts so the operator sees in the deploy
		// log how many rows the migration is about to flag, broken down by
		// reason. No mutation yet — if the numbers look wrong, the operator
		// can stop the container before STEPS B-D run.
		// Wrapped in tx + statement_timeout per CLAUDE.md Invariant #2 —
		// business_listings is 779K rows; a slow COUNT during deploy-restart
		// autovacuum can stall both pool conns and starve the API.
		var explicitOff, metaSoup int
		if preTx, ptxErr := db.Begin(); ptxErr != nil {
			slog.Warn("db: off-niche cleanup PRE-COUNT tx begin failed", "error", ptxErr)
		} else {
			preTx.Exec(`SET LOCAL statement_timeout = '5000'`) //nolint:errcheck — advisory
			_ = preTx.QueryRow(
				`SELECT COUNT(*) FROM business_listings WHERE category IN (` + offNicheTypes + `) AND off_niche IS NOT TRUE`,
			).Scan(&explicitOff)
			_ = preTx.QueryRow(
				`SELECT COUNT(*) FROM business_listings WHERE LENGTH(category) > 100 AND off_niche IS NOT TRUE`,
			).Scan(&metaSoup)
			preTx.Rollback() //nolint:errcheck — read-only, nothing to commit
		}
		slog.Info("db: off-niche cleanup pre-count",
			"explicit_off_niche_to_flag", explicitOff,
			"meta_keyword_soup_to_flag", metaSoup,
			"total_affected", explicitOff+metaSoup,
		)

		// STEP B — BACKUP: capture pre-mutation state for everything we're
		// about to flag. Append-safe via IF NOT EXISTS. Following the country
		// purge integrity gate pattern: count live rows, create backup, verify
		// backup count matches within 10% before mutating.
		var liveAffected int
		if err := db.QueryRow(`
			SELECT COUNT(*) FROM business_listings
			WHERE off_niche IS NOT TRUE
			  AND (category IN (` + offNicheTypes + `) OR LENGTH(category) > 100)
		`).Scan(&liveAffected); err != nil {
			slog.Warn("db: off-niche cleanup pre-backup count failed — aborting", "error", err)
			return nil
		}
		slog.Info("db: off-niche cleanup pre-backup count", "live_rows_to_flag", liveAffected)

		if _, err := db.Exec(`
			CREATE TABLE IF NOT EXISTS business_listings_offniche_backup_20260522 AS
			SELECT id, category, off_niche, niche_category, updated_at
			FROM business_listings
			WHERE off_niche IS NOT TRUE
			  AND (category IN (` + offNicheTypes + `) OR LENGTH(category) > 100)
		`); err != nil {
			slog.Warn("db: off-niche cleanup backup failed — aborting for safety", "error", err)
			return nil
		}
		var backupCount int
		_ = db.QueryRow(`SELECT COUNT(*) FROM business_listings_offniche_backup_20260522`).Scan(&backupCount)
		slog.Info("db: off-niche cleanup backup created", "backup_rows", backupCount, "expected_rows", liveAffected)

		if backupCount == 0 && liveAffected > 0 {
			slog.Warn("db: off-niche cleanup backup integrity FAILED — backup empty, aborting")
			return nil
		}
		if liveAffected > 0 {
			divergePct := float64(liveAffected-backupCount) / float64(liveAffected) * 100
			if divergePct < 0 {
				divergePct = -divergePct
			}
			if divergePct > 10 {
				slog.Warn("db: off-niche cleanup backup integrity FAILED — count diverges, aborting",
					"live_rows", liveAffected, "backup_rows", backupCount, "diverge_pct", divergePct)
				return nil
			}
		}
		slog.Info("db: off-niche cleanup backup integrity verified — proceeding with FLAG + CLASSIFY")

		// STEP C — FLAG: set off_niche=TRUE for the two patterns. Wrapped in
		// tx with statement_timeout (30s — partial idx_bl_niche_active helps,
		// but the UPDATE touches both indexed and unindexed rows).
		var flaggedRows int64
		if txB, txErr := db.Begin(); txErr != nil {
			slog.Warn("db: off-niche cleanup FLAG tx begin failed", "error", txErr)
		} else {
			txB.Exec(`SET LOCAL statement_timeout = '30000'`) //nolint:errcheck — advisory
			res, flagErr := txB.Exec(`
				UPDATE business_listings
				SET off_niche = TRUE, updated_at = NOW()
				WHERE off_niche IS NOT TRUE
				  AND (category IN (` + offNicheTypes + `) OR LENGTH(category) > 100)
			`)
			if flagErr != nil {
				slog.Warn("db: off-niche cleanup FLAG UPDATE failed — rolling back", "error", flagErr)
				txB.Rollback() //nolint:errcheck
			} else {
				txB.Commit() //nolint:errcheck
				flaggedRows, _ = res.RowsAffected()
				slog.Info("db: off-niche cleanup FLAG applied", "rows_flagged", flaggedRows)
			}
		}

		// STEP D — CLASSIFY: forward-fill niche_category for rows that survived
		// the FLAG step (off_niche=FALSE). Mirrors the trigger CASE so old rows
		// get the same classification as new rows. Single-pass UPDATE keyed by
		// the same regex; rows that match no niche stay NULL.
		var classifiedRows int64
		if txC, txErr := db.Begin(); txErr != nil {
			slog.Warn("db: off-niche cleanup CLASSIFY tx begin failed", "error", txErr)
		} else {
			txC.Exec(`SET LOCAL statement_timeout = '60000'`) //nolint:errcheck — advisory
			res, classErr := txC.Exec(`
				UPDATE business_listings SET niche_category = CASE
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\myoga|asana|vinyasa|ashtanga|kundalini|iyengar|hatha|bikram|jivamukti\M' THEN 'yoga'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\mpilates|reformer\M' THEN 'pilates'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\mcrossfit|bootcamp|hiit|barre|spin\M' THEN 'fitness'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\m(gym|fitness)\M' THEN 'fitness'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\mmeditation|mindfulness|breathwork\M' THEN 'meditation'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\mreiki|sound healing|energy healing|healing\M' THEN 'healing'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\mayurved\M' THEN 'ayurveda'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\m(spa|massage|thermal)\M' THEN 'spa'
				  WHEN LOWER(COALESCE(business_name,'') || ' ' ||
				             COALESCE(page_title,'') || ' ' ||
				             COALESCE(description,'')) ~ '\m(wellness|holistic)\M' THEN 'wellness'
				  ELSE niche_category
				END
				WHERE off_niche IS NOT TRUE
				  AND niche_category IS NULL
			`)
			if classErr != nil {
				slog.Warn("db: off-niche cleanup CLASSIFY UPDATE failed — rolling back", "error", classErr)
				txC.Rollback() //nolint:errcheck
			} else {
				txC.Commit() //nolint:errcheck
				classifiedRows, _ = res.RowsAffected()
				slog.Info("db: off-niche cleanup CLASSIFY applied", "rows_classified", classifiedRows)
			}
		}

		// STEP E — VERIFY. Wrapped in tx + statement_timeout per CLAUDE.md
		// Invariant #2 (same reason as PRE-COUNT above). The partial indexes
		// idx_bl_niche_active + idx_bl_off_niche_false back these COUNTs so
		// the planner should pick index-only scans, but the timeout is the
		// safety net for cases where the planner picks a seqscan instead
		// (e.g. statistics not yet refreshed after the bulk UPDATEs above).
		var finalOffNiche, finalClassified, finalUnclassified int
		if vTx, vtxErr := db.Begin(); vtxErr != nil {
			slog.Warn("db: off-niche cleanup VERIFY tx begin failed", "error", vtxErr)
		} else {
			vTx.Exec(`SET LOCAL statement_timeout = '5000'`) //nolint:errcheck — advisory
			_ = vTx.QueryRow(`SELECT COUNT(*) FROM business_listings WHERE off_niche IS TRUE`).Scan(&finalOffNiche)
			_ = vTx.QueryRow(`SELECT COUNT(*) FROM business_listings WHERE off_niche IS NOT TRUE AND niche_category IS NOT NULL`).Scan(&finalClassified)
			_ = vTx.QueryRow(`SELECT COUNT(*) FROM business_listings WHERE off_niche IS NOT TRUE AND niche_category IS NULL`).Scan(&finalUnclassified)
			vTx.Rollback() //nolint:errcheck — read-only, nothing to commit
		}
		slog.Info("db: off-niche cleanup VERIFY",
			"total_off_niche", finalOffNiche,
			"total_in_niche_classified", finalClassified,
			"total_in_niche_unclassified", finalUnclassified,
		)

		if _, err := db.Exec(
			`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
			offNicheCleanupVersion,
			"flag off_niche from category blacklist + meta-keyword-soup, then classify niche_category from name/title/description",
		); err != nil {
			slog.Warn("db: off-niche cleanup version record failed", "error", err)
		}
	}

	return nil
}

// migrateAdvisoryLockKey serializes Migrate() across all booting containers.
// Every container (serp×N, enrich×N, reenrich×N, manager) runs Migrate() on
// boot via cmd/run.go. Without serialization their concurrent DROP/CREATE
// TRIGGER + ALTER TABLE statements take ACCESS EXCLUSIVE locks that pile up
// and freeze serp_results / serp_jobs writes fleet-wide — the 2026-06-01
// reenrich-flap ↔ DDL-herd doom loop. Holding one session-level advisory lock
// for the whole migration means exactly one container migrates at a time; the
// rest block on pg_advisory_lock(), then proceed and find everything already
// present (fast IF NOT EXISTS / IF NOT EXISTS-guarded no-ops).
const migrateAdvisoryLockKey int64 = 778120601

// Migrate creates all tables if they don't exist, then runs incremental migrations.
//
// The whole body is serialized across containers by a session advisory lock
// held on a dedicated *sql.Conn (see migrateAdvisoryLockKey). The lock lives on
// `conn` for the duration; schema + runMigrations still execute on the pool,
// which is fine — only one container ever gets past pg_advisory_lock at a time.
func Migrate(db *sql.DB) error {
	ctx := context.Background()

	conn, err := db.Conn(ctx)
	if err != nil {
		return fmt.Errorf("db: migrate: acquire conn: %w", err)
	}
	defer conn.Close()

	// pg_advisory_lock() BLOCKS until the lock is free, which routinely exceeds
	// the server statement_timeout (60s) while another container is migrating.
	// The timeout MUST be disabled on this connection, or the lock wait is
	// cancelled with 57014 → Migrate() errors → the process exits → every
	// non-first container crash-loops on boot and the manager API never starts
	// (exactly what v0.8.7 hit in prod 2026-06-01). Scoped to this conn only.
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = 0`); err != nil {
		return fmt.Errorf("db: migrate: disable conn timeout: %w", err)
	}
	if _, err := conn.ExecContext(ctx, `SELECT pg_advisory_lock($1)`, migrateAdvisoryLockKey); err != nil {
		return fmt.Errorf("db: migrate: advisory lock: %w", err)
	}
	defer func() {
		// Best-effort explicit release; closing the conn also drops the session lock.
		_, _ = conn.ExecContext(context.Background(), `SELECT pg_advisory_unlock($1)`, migrateAdvisoryLockKey)
	}()

	db.Exec(`SET statement_timeout = '300s'`)
	defer db.Exec(`SET statement_timeout = '60s'`)

	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("db: migration failed: %w", err)
	}
	return runMigrations(db)
}

// completenessBackfillVersion gates the one-time completeness_score backfill.
const completenessBackfillVersion = "2026_06_02_completeness_score_backfill"

// BackfillCompletenessScore populates business_listings.completeness_score for
// existing un-reenriched rows ONCE (version-gated). Runs in the BACKGROUND
// (manager only) so it never blocks the fleet boot — the reenrich worker just
// picks up rows as they get scored, and a NULL score is treated as not-yet-
// eligible. Scoped to re_enriched_at IS NULL (the only rows the eligibility
// query reads), so it touches ~the reenrich pool, not all of business_listings.
// Set-based: the acceptable-email semi-join is evaluated once. Uses the same
// 0-100 rubric as trg_normalize_enrichment so backfilled and trigger-maintained
// scores stay consistent.
func BackfillCompletenessScore(ctx context.Context, db *sql.DB) {
	var done bool
	if err := db.QueryRowContext(ctx,
		`SELECT EXISTS(SELECT 1 FROM schema_migrations WHERE version = $1)`,
		completenessBackfillVersion,
	).Scan(&done); err != nil {
		slog.Warn("db: completeness backfill version check failed", "error", err)
		return
	}
	if done {
		return
	}

	conn, err := db.Conn(ctx)
	if err != nil {
		slog.Warn("db: completeness backfill: acquire conn failed", "error", err)
		return
	}
	defer conn.Close()
	// Long timeout — a one-time scoped UPDATE run in the background.
	if _, err := conn.ExecContext(ctx, `SET statement_timeout = '900s'`); err != nil {
		slog.Warn("db: completeness backfill: set timeout failed", "error", err)
		return
	}

	slog.Info("db: completeness_score backfill starting (background, one-time)")
	res, err := conn.ExecContext(ctx, `
		UPDATE business_listings bl SET completeness_score =
		    (CASE WHEN bl.id IN (
		       SELECT be.business_id FROM business_emails be JOIN emails e ON e.id = be.email_id
		       WHERE e.is_acceptable = true OR e.score >= 0.7
		     ) THEN 40 ELSE 0 END)
		  + CASE WHEN (bl.phone IS NOT NULL AND bl.phone != '')
		        OR (bl.phones IS NOT NULL AND array_length(bl.phones, 1) > 0) THEN 20 ELSE 0 END
		  + CASE WHEN (bl.business_name IS NOT NULL AND bl.business_name != '')
		        AND (bl.category IS NOT NULL AND bl.category != '') THEN 15 ELSE 0 END
		  + CASE WHEN (bl.address IS NOT NULL AND bl.address != '')
		        OR ((bl.city IS NOT NULL AND bl.city != '') AND (bl.country IS NOT NULL AND bl.country != '')) THEN 15 ELSE 0 END
		  + CASE WHEN bl.social_links IS NOT NULL AND bl.social_links != '{}'::jsonb THEN 10 ELSE 0 END
		WHERE bl.completeness_score IS NULL AND bl.re_enriched_at IS NULL
	`)
	if err != nil {
		slog.Warn("db: completeness_score backfill failed — reenrich eligibility limited until a later boot retries", "error", err)
		return
	}
	n, _ := res.RowsAffected()
	if _, err := db.ExecContext(ctx,
		`INSERT INTO schema_migrations (version, notes) VALUES ($1, $2) ON CONFLICT (version) DO NOTHING`,
		completenessBackfillVersion, "backfill business_listings.completeness_score for reenrich eligibility",
	); err != nil {
		slog.Warn("db: completeness backfill: version record failed", "error", err)
	}
	slog.Info("db: completeness_score backfill done", "rows_scored", n)
}
