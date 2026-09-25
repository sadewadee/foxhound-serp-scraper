package stage

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"os"
	"os/exec"
	"strings"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

// TestReconcileSQLShape locks the invariants of the reconciler queries without a
// database: the batch is bounded, runs under a statement timeout, and completion
// no longer depends on a recency window.
func TestReconcileSQLShape(t *testing.T) {
	src, err := os.ReadFile("serp_reconcile.go")
	if err != nil {
		t.Fatalf("read serp_reconcile.go: %v", err)
	}
	body := string(src)

	fn := body[strings.Index(body, "func ReconcileProcessingQueries"):]
	if i := strings.Index(fn, "\nfunc "); i > 0 {
		fn = fn[:i]
	}

	for _, want := range []string{
		"SET LOCAL statement_timeout = '5000'",
		"LIMIT 500",
		"ORDER BY updated_at ASC",
		"engine disabled in SERP_ENGINES",
		"all serp jobs failed/dead with 0 results",
	} {
		if !strings.Contains(fn, want) {
			t.Errorf("ReconcileProcessingQueries missing %q", want)
		}
	}
	if strings.Contains(fn, "INTERVAL '2 minutes'") {
		t.Error("completion still gated on the 2-minute recency window")
	}
	if strings.Contains(fn, "pendingCount") {
		t.Error("requeue still gated on the pending-count threshold")
	}
}

// TestReconcileProcessingQueries_Integration seeds the five query states the
// reconciler must distinguish and runs one pass against a throwaway Postgres.
func TestReconcileProcessingQueries_Integration(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}

	user := "xxchk_" + randHex(t, 3)
	pass := randB64(t)
	port := freePort(t)
	name := "xxchk_reconcile_" + randHex(t, 3)

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
	t.Cleanup(func() {
		_ = exec.Command("docker", "rm", "-f", name).Run()
	})

	// Credentials are random and may contain URL-reserved characters.
	dsn := fmt.Sprintf("postgres://%s:%s@127.0.0.1:%s/xxchk?sslmode=disable",
		url.QueryEscape(user), url.QueryEscape(pass), port)
	db := waitForDB(t, dsn)
	defer db.Close()

	if _, err := db.Exec(reconcileTestSchema); err != nil {
		t.Fatalf("schema: %v", err)
	}

	seed := []struct {
		text   string
		status string
	}{
		{"q-all-completed", "processing"},
		{"q-all-dead", "processing"},
		{"q-zombie", "processing"},
		{"q-disabled-engine", "processing"},
		{"q-still-new", "processing"},
	}
	ids := map[string]int64{}
	for _, s := range seed {
		var id int64
		if err := db.QueryRow(
			`INSERT INTO queries (text, text_hash, status) VALUES ($1, $2, $3) RETURNING id`,
			s.text, s.text, s.status).Scan(&id); err != nil {
			t.Fatalf("seed query %s: %v", s.text, err)
		}
		ids[s.text] = id
	}

	jobs := []struct {
		query   string
		engine  string
		status  string
		results int
		suffix  string
	}{
		{"q-all-completed", "bing", "completed", 7, "a"},
		{"q-all-completed", "duckduckgo", "completed", 3, "b"},
		{"q-all-dead", "bing", "dead", 0, "a"},
		{"q-all-dead", "duckduckgo", "failed", 0, "b"},
		{"q-disabled-engine", "google", "new", 0, "a"},
		{"q-disabled-engine", "bing", "completed", 5, "b"},
		{"q-still-new", "bing", "completed", 4, "a"},
		{"q-still-new", "duckduckgo", "new", 0, "b"},
	}
	for _, j := range jobs {
		if _, err := db.Exec(
			`INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, engine, status, result_count)
			 VALUES ($1, $2, $3, 0, $4, $5, $6)`,
			fmt.Sprintf("%s-%s", j.query, j.suffix), ids[j.query], "http://example/"+j.query, j.engine, j.status, j.results,
		); err != nil {
			t.Fatalf("seed job %s-%s: %v", j.query, j.suffix, err)
		}
	}

	res, err := ReconcileProcessingQueries(context.Background(), db, []string{"bing", "duckduckgo"})
	if err != nil {
		t.Fatalf("reconcile: %v", err)
	}
	if res.Completed != 2 || res.Failed != 1 || res.Requeued != 1 || res.Active != 1 {
		t.Errorf("result = %+v; want completed=2 failed=1 requeued=1 active=1", res)
	}

	want := map[string]string{
		"q-all-completed":   "completed",
		"q-all-dead":        "failed",
		"q-zombie":          "pending",
		"q-disabled-engine": "completed",
		"q-still-new":       "processing",
	}
	for text, status := range want {
		var got string
		var resultCount int
		if err := db.QueryRow(`SELECT status, result_count FROM queries WHERE id = $1`, ids[text]).
			Scan(&got, &resultCount); err != nil {
			t.Fatalf("read %s: %v", text, err)
		}
		if got != status {
			t.Errorf("%s status = %q, want %q", text, got, status)
		}
		if text == "q-all-completed" && resultCount != 10 {
			t.Errorf("%s result_count = %d, want 10", text, resultCount)
		}
		if text == "q-disabled-engine" && resultCount != 5 {
			t.Errorf("%s result_count = %d, want 5", text, resultCount)
		}
	}

	var googleStatus, googleErr string
	if err := db.QueryRow(
		`SELECT status, COALESCE(error_msg,'') FROM serp_jobs WHERE id = 'q-disabled-engine-a'`).
		Scan(&googleStatus, &googleErr); err != nil {
		t.Fatalf("read disabled-engine job: %v", err)
	}
	if googleStatus != "dead" || googleErr != "engine disabled in SERP_ENGINES" {
		t.Errorf("disabled-engine job = %s/%q, want dead/'engine disabled in SERP_ENGINES'", googleStatus, googleErr)
	}

	var ddgStatus string
	if err := db.QueryRow(`SELECT status FROM serp_jobs WHERE id = 'q-still-new-b'`).Scan(&ddgStatus); err != nil {
		t.Fatalf("read still-new job: %v", err)
	}
	if ddgStatus != "new" {
		t.Errorf("still-new job status = %q, want 'new' (must not be touched)", ddgStatus)
	}
}

const reconcileTestSchema = `
CREATE TABLE queries (
    id           BIGSERIAL PRIMARY KEY,
    text         TEXT NOT NULL,
    text_hash    TEXT NOT NULL,
    status       TEXT NOT NULL DEFAULT 'pending',
    result_count INTEGER DEFAULT 0,
    error_msg    TEXT,
    updated_at   TIMESTAMPTZ DEFAULT NOW()
);
CREATE TABLE serp_jobs (
    id            TEXT PRIMARY KEY,
    parent_job_id BIGINT NOT NULL REFERENCES queries(id),
    search_url    TEXT NOT NULL,
    page_num      INTEGER NOT NULL,
    engine        TEXT DEFAULT 'google',
    status        TEXT NOT NULL DEFAULT 'new',
    result_count  INTEGER DEFAULT 0,
    error_msg     TEXT,
    updated_at    TIMESTAMPTZ DEFAULT NOW()
);`

func randHex(t *testing.T, n int) string {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return hex.EncodeToString(b)
}

func randB64(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("openssl", "rand", "-base64", "24").Output()
	if err != nil {
		t.Fatalf("openssl rand: %v", err)
	}
	return strings.TrimSpace(string(out))
}

func freePort(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return fmt.Sprintf("%d", ln.Addr().(*net.TCPAddr).Port)
}

func waitForDB(t *testing.T, dsn string) *sql.DB {
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
