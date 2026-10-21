package db

import (
	"database/sql"
	"os"
	"testing"
	"time"
)

// TestMigrateLockSafe_Integration needs a throwaway Postgres; it is skipped
// unless MIGRATE_TEST_DSN is set. NEVER point it at a real database.
func TestMigrateLockSafe_Integration(t *testing.T) {
	dsn := os.Getenv("MIGRATE_TEST_DSN")
	if dsn == "" {
		t.Skip("MIGRATE_TEST_DSN not set")
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	timed := func(label string) time.Duration {
		start := time.Now()
		if err := Migrate(db); err != nil {
			t.Fatalf("%s: %v", label, err)
		}
		d := time.Since(start)
		t.Logf("%s: %s", label, d)
		return d
	}

	timed("migrate #1 (fresh or existing)")
	timed("migrate #2 (already migrated, no contention)")

	// Live-writer simulation: open txs holding ROW EXCLUSIVE, like enrich workers.
	conn, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	tx, err := conn.Begin()
	if err != nil {
		t.Fatal(err)
	}
	defer tx.Rollback()
	for _, tbl := range []string{"business_listings", "enrichment_jobs"} {
		if _, err := tx.Exec(`LOCK TABLE ` + tbl + ` IN ROW EXCLUSIVE MODE`); err != nil {
			t.Fatal(err)
		}
	}
	if d := timed("migrate #3 (writers holding ROW EXCLUSIVE)"); d > 10*time.Second {
		t.Errorf("migrate waited on writer locks: %s", d)
	}
	tx.Rollback()

	// A genuinely missing column must be restored.
	if _, err := db.Exec(`ALTER TABLE business_listings DROP COLUMN IF EXISTS tiktok`); err != nil {
		t.Fatal(err)
	}
	timed("migrate #4 (tiktok dropped)")
	var n int
	if err := db.QueryRow(`SELECT count(*) FROM information_schema.columns WHERE table_name='business_listings' AND column_name='tiktok'`).Scan(&n); err != nil || n != 1 {
		t.Fatalf("tiktok column not restored: n=%d err=%v", n, err)
	}
}
