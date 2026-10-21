package db

import (
	"errors"
	"fmt"
	"testing"

	pq "github.com/lib/pq"
)

func TestParseDDLTarget(t *testing.T) {
	cases := []struct {
		stmt        string
		table, name string
		ok          bool
	}{
		{`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS contact_name TEXT`, "business_listings", "contact_name", true},
		{`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS phones TEXT[] DEFAULT '{}'`, "business_listings", "phones", true},
		{`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS off_niche BOOLEAN DEFAULT FALSE`, "business_listings", "off_niche", true},
		{`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS country_code CHAR(2)`, "business_listings", "country_code", true},
		{"\n\t\talter table   workers\n add column if not exists delta_at TIMESTAMPTZ", "workers", "delta_at", true},
		{`CREATE INDEX IF NOT EXISTS idx_emails_created_at ON emails(created_at)`, "", "idx_emails_created_at", true},
		{`CREATE INDEX IF NOT EXISTS idx_bl_name_trgm ON business_listings USING gin(business_name gin_trgm_ops)`, "", "idx_bl_name_trgm", true},
		{`CREATE UNIQUE INDEX IF NOT EXISTS idx_u ON t (a)`, "", "idx_u", true},
		{`CREATE INDEX IF NOT EXISTS idx_serp_feed ON serp_jobs(created_at) WHERE status = 'new' AND picked_at IS NULL`, "", "idx_serp_feed", true},
		// Not recognised -> must execute.
		{`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_x ON t (a)`, "", "", false},
		{`CREATE INDEX idx_category_stats_biz_count ON category_stats (biz_count DESC)`, "", "", false},
		{`ALTER TABLE business_listings SET (autovacuum_vacuum_scale_factor = 0.02)`, "", "", false},
		{`ALTER TABLE t ADD COLUMN c TEXT`, "", "", false},
		{`UPDATE emails SET domain = 'x'`, "", "", false},
		{`DROP MATERIALIZED VIEW IF EXISTS category_stats`, "", "", false},
	}
	for _, c := range cases {
		table, name, ok := parseDDLTarget(c.stmt)
		if table != c.table || name != c.name || ok != c.ok {
			t.Errorf("parseDDLTarget(%q) = (%q,%q,%v), want (%q,%q,%v)", c.stmt, table, name, ok, c.table, c.name, c.ok)
		}
	}
}

func TestExtractSchemaRelations(t *testing.T) {
	got := map[string]bool{}
	for _, n := range schemaRelations {
		got[n] = true
	}
	for _, want := range []string{
		"queries", "serp_jobs", "serp_results", "enrichment_jobs", "business_listings",
		"emails", "business_emails", "workers", "idx_enrich_feed", "idx_enrich_completed_at", "idx_serp_jobs_claim",
	} {
		if !got[want] {
			t.Errorf("schemaRelations missing %q", want)
		}
	}
	// Created later in runMigrations, not by the schema block.
	for _, notWant := range []string{"idx_serp_feed", "idx_serp_stale"} {
		if got[notWant] {
			t.Errorf("schemaRelations unexpectedly contains %q", notWant)
		}
	}
	if len(extractSchemaRelations(`SELECT 1`)) != 0 {
		t.Error("expected no relations from non-DDL text")
	}
	if r := extractSchemaRelations(`CREATE UNIQUE INDEX IF NOT EXISTS ix_a ON t(a); CREATE MATERIALIZED VIEW IF NOT EXISTS mv AS SELECT 1`); len(r) != 2 || r[0] != "ix_a" || r[1] != "mv" {
		t.Errorf("unexpected extraction: %v", r)
	}
}

func TestIsRetryableDDLErr(t *testing.T) {
	for code, want := range map[pq.ErrorCode]bool{"55P03": true, "40P01": true, "42P01": false} {
		if got := isRetryableDDLErr(fmt.Errorf("wrap: %w", &pq.Error{Code: code})); got != want {
			t.Errorf("code %s: got %v want %v", code, got, want)
		}
	}
	if isRetryableDDLErr(errors.New("plain")) {
		t.Error("plain error must not be retryable")
	}
}
