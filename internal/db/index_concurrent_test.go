package db

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
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
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}

	user := "xxchk_" + icRandHex(t, 3)
	pass := icRandB64(t)
	port := icFreePort(t)
	name := "xxchk_idx_" + icRandHex(t, 3)

	run := exec.Command("docker", "run", "--rm", "-d",
		"--name", name,
		"-p", "127.0.0.1:"+port+":5432",
		"-e", "POSTGRES_USER="+user,
		"-e", "POSTGRES_PASSWORD="+pass,
		"-e", "POSTGRES_DB=xxchk",
		"postgres:17")
	if out, err := run.CombinedOutput(); err != nil {
		t.Fatalf("start postgres: %v\n%s", err, out)
	}
	t.Cleanup(func() { _ = exec.Command("docker", "rm", "-f", name).Run() })

	u := &url.URL{Scheme: "postgres", User: url.UserPassword(user, pass), Host: "127.0.0.1:" + port, Path: "/xxchk", RawQuery: "sslmode=disable"}
	dsn := u.String()
	db := icWaitForDB(t, dsn)
	defer db.Close()

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

func icRandHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func icRandB64(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("openssl", "rand", "-base64", "24").Output()
	if err != nil {
		t.Fatalf("openssl rand: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func icFreePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return fmt.Sprintf("%d", ln.Addr().(*net.TCPAddr).Port)
}

func icWaitForDB(t *testing.T, dsn string) *sql.DB {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	var lastErr error
	for time.Now().Before(deadline) {
		db, err := sql.Open("postgres", dsn)
		if err == nil {
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			err = db.PingContext(ctx)
			cancel()
			if err == nil {
				return db
			}
			db.Close()
		}
		lastErr = err
		time.Sleep(500 * time.Millisecond)
	}
	t.Fatalf("postgres never became ready: %v", lastErr)
	return nil
}
