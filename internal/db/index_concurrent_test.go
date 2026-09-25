package db

import (
	"testing"

	"github.com/sadewadee/serp-scraper/internal/testpg"
)

// TestIndexBuildDecision locks the self-healing rule for concurrently built
// indexes: a missing index is created, a valid one is left alone, and an
// INVALID one (left behind when a CREATE INDEX CONCURRENTLY is interrupted,
// e.g. by the manager container restarting mid-build) is dropped and rebuilt
// — IF NOT EXISTS would otherwise preserve the broken index forever.
func TestIndexBuildDecision(t *testing.T) {
	tests := []struct {
		name        string
		exists      bool
		valid       bool
		want        indexBuildAction
		description string
	}{
		{"absent", false, false, indexBuildCreate, "fresh database, nothing to drop"},
		{"valid", true, true, indexBuildSkip, "already built, boot must be a no-op"},
		{"invalid", true, false, indexBuildRebuild, "interrupted build left an unusable index"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := indexBuildDecision(tt.exists, tt.valid); got != tt.want {
				t.Errorf("indexBuildDecision(exists=%v, valid=%v) = %v, want %v (%s)",
					tt.exists, tt.valid, got, tt.want, tt.description)
			}
		})
	}
}

// TestEnsureIndexConcurrently_RebuildsInvalid drives the real helper against a
// throwaway Postgres: an index marked indisvalid=false (what an interrupted
// CREATE INDEX CONCURRENTLY leaves behind) must come back valid after one call.
func TestEnsureIndexConcurrently_RebuildsInvalid(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_idx_"}).DB()

	const idx = "idx_serp_claim_engine"
	if _, err := db.Exec(`CREATE TABLE serp_jobs (engine TEXT, priority INT, created_at TIMESTAMPTZ, status TEXT)`); err != nil {
		t.Fatalf("create table: %v", err)
	}
	if _, err := db.Exec(`CREATE INDEX ` + idx + ` ON serp_jobs (engine, priority DESC, created_at) WHERE status = 'new'`); err != nil {
		t.Fatalf("create index: %v", err)
	}
	if _, err := db.Exec(`UPDATE pg_index SET indisvalid = false WHERE indexrelid = '` + idx + `'::regclass`); err != nil {
		t.Fatalf("invalidate index: %v", err)
	}

	ensureIndexConcurrently(db, idx,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS `+idx+` ON serp_jobs (engine, priority DESC, created_at) WHERE status = 'new'`)

	var valid bool
	if err := db.QueryRow(`
		SELECT i.indisvalid FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1`, idx).Scan(&valid); err != nil {
		t.Fatalf("read validity: %v", err)
	}
	if !valid {
		t.Error("index still invalid after ensureIndexConcurrently — invalid index was not rebuilt")
	}

	// Second call must be a no-op that leaves the valid index in place.
	ensureIndexConcurrently(db, idx,
		`CREATE INDEX CONCURRENTLY IF NOT EXISTS `+idx+` ON serp_jobs (engine, priority DESC, created_at) WHERE status = 'new'`)
	if err := db.QueryRow(`
		SELECT i.indisvalid FROM pg_index i
		JOIN pg_class c ON c.oid = i.indexrelid
		WHERE c.relname = $1`, idx).Scan(&valid); err != nil || !valid {
		t.Errorf("index not valid after second (no-op) call: valid=%v err=%v", valid, err)
	}
}
