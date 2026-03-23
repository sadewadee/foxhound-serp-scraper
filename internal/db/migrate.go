package db

import (
	"database/sql"
	"fmt"
)

const schema = `
CREATE TABLE IF NOT EXISTS queries (
    id           BIGSERIAL PRIMARY KEY,
    text         TEXT NOT NULL,
    text_hash    TEXT NOT NULL,
    template_id  TEXT,
    status       TEXT NOT NULL DEFAULT 'pending',
    result_count INTEGER DEFAULT 0,
    error_msg    TEXT,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    updated_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(text_hash)
);
CREATE INDEX IF NOT EXISTS idx_queries_status ON queries(status);

CREATE TABLE IF NOT EXISTS serp_seeds (
    id           BIGSERIAL PRIMARY KEY,
    query_id     BIGINT NOT NULL REFERENCES queries(id),
    google_url   TEXT NOT NULL,
    page_num     INTEGER NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    result_count INTEGER DEFAULT 0,
    created_at   TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(query_id, page_num)
);
CREATE INDEX IF NOT EXISTS idx_serp_seeds_status ON serp_seeds(status);

CREATE TABLE IF NOT EXISTS websites (
    id              BIGSERIAL PRIMARY KEY,
    domain          TEXT NOT NULL,
    url             TEXT NOT NULL,
    url_hash        TEXT NOT NULL,
    source_query_id BIGINT REFERENCES queries(id),
    source_serp_id  BIGINT REFERENCES serp_seeds(id),
    page_type       TEXT DEFAULT 'serp_result',
    status          TEXT NOT NULL DEFAULT 'pending',
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(url_hash)
);
CREATE INDEX IF NOT EXISTS idx_websites_status ON websites(status);
CREATE INDEX IF NOT EXISTS idx_websites_domain ON websites(domain);

CREATE TABLE IF NOT EXISTS contacts (
    id              BIGSERIAL PRIMARY KEY,
    email           TEXT,
    email_hash      TEXT,
    phone           TEXT,
    domain          TEXT NOT NULL,
    website_id      BIGINT REFERENCES websites(id),
    source_url      TEXT NOT NULL,
    source_query_id BIGINT REFERENCES queries(id),
    instagram       TEXT,
    facebook        TEXT,
    twitter         TEXT,
    linkedin        TEXT,
    whatsapp        TEXT,
    address         TEXT,
    raw_context     TEXT,
    mx_valid        BOOLEAN,
    created_at      TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(email_hash, domain)
);
CREATE INDEX IF NOT EXISTS idx_contacts_email_hash ON contacts(email_hash);
CREATE INDEX IF NOT EXISTS idx_contacts_domain ON contacts(domain);

CREATE TABLE IF NOT EXISTS pipeline_state (
    key        TEXT PRIMARY KEY,
    value      TEXT NOT NULL,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);
`

// Migrate creates all tables if they don't exist.
func Migrate(db *sql.DB) error {
	if _, err := db.Exec(schema); err != nil {
		return fmt.Errorf("db: migration failed: %w", err)
	}
	return nil
}
