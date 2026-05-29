package db

import "testing"

// Guards the boot-migration lock-stall fix (incident 2026-05-29): addColumnRe must
// reliably pull <table> and <column> out of an "ALTER TABLE ... ADD COLUMN IF NOT
// EXISTS ..." statement so execMigrationBatch can skip the no-op (and its
// ACCESS EXCLUSIVE lock on hot tables) when the column already exists. It must NOT
// match other DDL (CREATE INDEX, plain ALTER) — those always run.
func TestAddColumnRe(t *testing.T) {
	matches := map[string][2]string{
		`ALTER TABLE serp_jobs ADD COLUMN IF NOT EXISTS picked_at TIMESTAMPTZ`:              {"serp_jobs", "picked_at"},
		`ALTER TABLE enrichment_jobs ADD COLUMN IF NOT EXISTS raw_country TEXT`:             {"enrichment_jobs", "raw_country"},
		`ALTER TABLE business_listings ADD COLUMN IF NOT EXISTS phones TEXT[] DEFAULT '{}'`: {"business_listings", "phones"},
		"\t\tALTER TABLE workers ADD COLUMN IF NOT EXISTS pages_prev BIGINT DEFAULT 0":      {"workers", "pages_prev"},
		`alter table foo add column if not exists bar int`:                                  {"foo", "bar"}, // case-insensitive
	}
	for stmt, want := range matches {
		m := addColumnRe.FindStringSubmatch(stmt)
		if m == nil {
			t.Errorf("expected match for %q, got none", stmt)
			continue
		}
		if m[1] != want[0] || m[2] != want[1] {
			t.Errorf("for %q: got table=%q col=%q, want table=%q col=%q", stmt, m[1], m[2], want[0], want[1])
		}
	}

	nonMatches := []string{
		`CREATE INDEX IF NOT EXISTS idx_bl_country ON business_listings(country) WHERE country IS NOT NULL`,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS idx_bl_niche_active ON business_listings(niche_category)`,
		`ALTER TABLE serp_jobs ADD COLUMN picked_at TIMESTAMPTZ`, // no IF NOT EXISTS -> not our skip case
		`ALTER TABLE IF EXISTS enrich_jobs RENAME TO enrich_jobs_backup`,
		`UPDATE emails SET domain = split_part(email, '@', 2)`,
	}
	for _, stmt := range nonMatches {
		if m := addColumnRe.FindStringSubmatch(stmt); m != nil {
			t.Errorf("expected NO match for %q, got table=%q col=%q", stmt, m[1], m[2])
		}
	}
}
