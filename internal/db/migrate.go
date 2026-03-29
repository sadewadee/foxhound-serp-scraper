package db

import (
	"database/sql"
	"fmt"
)

const schema = `
CREATE TABLE IF NOT EXISTS queries (
    id              BIGSERIAL PRIMARY KEY,
    text            TEXT NOT NULL,
    text_hash       TEXT NOT NULL,
    status          TEXT NOT NULL DEFAULT 'pending',
    result_count    INTEGER DEFAULT 0,
    error_msg       TEXT,
    expanded_at     TIMESTAMPTZ,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(text_hash)
);
CREATE INDEX IF NOT EXISTS idx_queries_status ON queries(status);
-- For auto-expansion: find completed queries not yet expanded.
CREATE INDEX IF NOT EXISTS idx_queries_expand ON queries(status, expanded_at) WHERE status = 'completed' AND expanded_at IS NULL;

CREATE TABLE IF NOT EXISTS serp_jobs (
    id              TEXT PRIMARY KEY,
    parent_job_id   BIGINT NOT NULL REFERENCES queries(id),
    priority        INTEGER DEFAULT 0,
    search_url      TEXT NOT NULL,
    page_num        INTEGER NOT NULL,
    status          TEXT NOT NULL DEFAULT 'new',
    attempt_count   INTEGER NOT NULL DEFAULT 0,
    max_attempts    INTEGER NOT NULL DEFAULT 3,
    next_attempt_at TIMESTAMPTZ,
    locked_by       TEXT,
    locked_at       TIMESTAMPTZ,
    result_count    INTEGER DEFAULT 0,
    error_msg       TEXT,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS idx_serp_jobs_claim ON serp_jobs(parent_job_id, status, priority DESC, created_at) WHERE status = 'new';
CREATE INDEX IF NOT EXISTS idx_serp_jobs_parent ON serp_jobs(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_serp_jobs_status ON serp_jobs(status);

CREATE TABLE IF NOT EXISTS websites (
    id              BIGSERIAL PRIMARY KEY,
    domain          TEXT NOT NULL,
    url             TEXT NOT NULL,
    url_hash        TEXT NOT NULL,
    source_query_id BIGINT REFERENCES queries(id),
    source_serp_id  TEXT REFERENCES serp_jobs(id),
    page_type       TEXT DEFAULT 'serp_result',
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    updated_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(url_hash)
);
CREATE INDEX IF NOT EXISTS idx_websites_status ON websites(status);
CREATE INDEX IF NOT EXISTS idx_websites_domain ON websites(domain);

CREATE TABLE IF NOT EXISTS enrich_jobs (
    id                  UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    parent_job_id       BIGINT REFERENCES queries(id),
    source_website_id   BIGINT REFERENCES websites(id),
    domain              TEXT NOT NULL,
    url                 TEXT NOT NULL,
    url_hash            TEXT NOT NULL,
    status              TEXT NOT NULL DEFAULT 'pending',
    attempt_count       INTEGER NOT NULL DEFAULT 0,
    max_attempts        INTEGER NOT NULL DEFAULT 5,
    next_attempt_at     TIMESTAMPTZ DEFAULT NOW(),
    locked_by           TEXT,
    locked_at           TIMESTAMPTZ,
    error_msg           TEXT,
    contact_name        TEXT,
    business_name       TEXT,
    business_category   TEXT,
    description         TEXT,
    website             TEXT,
    emails              TEXT[] DEFAULT '{}',
    phones              TEXT[] DEFAULT '{}',
    social_links        JSONB DEFAULT '{}',
    address             TEXT,
    location            TEXT,
    opening_hours       TEXT,
    rating              TEXT,
    page_title          TEXT,
    mx_valid            BOOLEAN,
    created_at          TIMESTAMPTZ DEFAULT NOW(),
    updated_at          TIMESTAMPTZ DEFAULT NOW(),
    completed_at        TIMESTAMPTZ,
    UNIQUE(url_hash)
);
CREATE INDEX IF NOT EXISTS idx_enrich_claim ON enrich_jobs(status, next_attempt_at, created_at) WHERE status IN ('pending', 'failed');
CREATE INDEX IF NOT EXISTS idx_enrich_domain ON enrich_jobs(domain);
CREATE INDEX IF NOT EXISTS idx_enrich_parent ON enrich_jobs(parent_job_id);
CREATE INDEX IF NOT EXISTS idx_enrich_status_domain ON enrich_jobs(status, domain);
CREATE INDEX IF NOT EXISTS idx_enrich_completed_at ON enrich_jobs(completed_at) WHERE status = 'completed';
CREATE INDEX IF NOT EXISTS idx_serp_jobs_updated_at ON serp_jobs(updated_at) WHERE status = 'completed';

`

// runMigrations applies incremental schema changes that are safe to re-run.
func runMigrations(db *sql.DB) error {
	// v2: query auto-expansion tracking
	db.Exec(`ALTER TABLE queries ADD COLUMN expanded_at TIMESTAMPTZ`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_queries_expand ON queries(status, expanded_at) WHERE status = 'completed' AND expanded_at IS NULL`)

	// v2: indexes for reconciler performance
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_enrich_locked ON enrich_jobs(locked_at) WHERE status = 'processing'`)
	db.Exec(`CREATE INDEX IF NOT EXISTS idx_serp_locked ON serp_jobs(locked_at) WHERE status = 'processing'`)

	return nil
}

// Migrate creates all tables if they don't exist, then runs incremental migrations.
func Migrate(db *sql.DB) error {
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("db: migration failed: %w", err)
	}
	return runMigrations(db)
}
