package db

import (
	"database/sql"
	"fmt"
	"log/slog"
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
	} {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("db: schema fix 2026-04-27: %w (stmt: %s)", err, stmt)
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
	if _, err := db.Exec(`
		DROP TRIGGER IF EXISTS trg_serp_results_enqueue ON serp_results;
		CREATE TRIGGER trg_serp_results_enqueue
		  AFTER INSERT ON serp_results
		  FOR EACH ROW EXECUTE FUNCTION trg_enqueue_enrichment();
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
		    INSERT INTO business_listings (domain, url, business_name, category, description,
		        address, location, country, city, contact_name,
		        phone, phones, website, page_title, social_links,
		        opening_hours, rating, tiktok, youtube, telegram, source_query_id)
		    VALUES (NEW.domain, NEW.url, NEW.raw_business_name, NEW.raw_category, NEW.raw_description,
		        NEW.raw_address, NEW.raw_location, NEW.raw_country, NEW.raw_city, NEW.raw_contact_name,
		        NEW.raw_phones[1], COALESCE(NEW.raw_phones, '{}'), NEW.url, NEW.raw_page_title, NEW.raw_social,
		        NEW.raw_opening_hours, NEW.raw_rating, NEW.raw_tiktok, NEW.raw_youtube, NEW.raw_telegram,
		        NEW.parent_query_id)
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
	if _, err := db.Exec(`
		DROP TRIGGER IF EXISTS trg_enrichment_normalize ON enrichment_jobs;
		CREATE TRIGGER trg_enrichment_normalize
		  AFTER UPDATE ON enrichment_jobs
		  FOR EACH ROW
		  WHEN (NEW.status = 'completed' AND OLD.status IS DISTINCT FROM 'completed')
		  EXECUTE FUNCTION trg_normalize_enrichment();
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

	return nil
}

// Migrate creates all tables if they don't exist, then runs incremental migrations.
func Migrate(db *sql.DB) error {
	db.Exec(`SET statement_timeout = '300s'`)
	defer db.Exec(`SET statement_timeout = '60s'`)

	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("db: migration failed: %w", err)
	}
	return runMigrations(db)
}
