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
		"FOR UPDATE SKIP LOCKED",
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

	db := startReconcilePostgres(t)
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

// TestReconcileProcessingQueries_ConcurrentCallers is the regression test for
// the prod contention seen after #54 shipped: ReconcileProcessingQueries runs
// in every serp container on both hosts, and without FOR UPDATE SKIP LOCKED
// both callers took the identical "500 oldest processing" batch and then
// blocked on each other's row locks until the 5s statement_timeout fired
// (`serp: advance active queries: canceling statement due to statement
// timeout (57014)`, ~9x/3h). With SKIP LOCKED the second caller skips the
// locked rows and picks a disjoint batch, so both finish without timing out
// and the union of their work equals a single serial pass.
func TestReconcileProcessingQueries_ConcurrentCallers(t *testing.T) {
	if _, err := exec.LookPath("docker"); err != nil {
		t.Skip("docker not available")
	}
	db := startReconcilePostgres(t)
	if _, err := db.Exec(reconcileTestSchema); err != nil {
		t.Fatalf("schema: %v", err)
	}

	// 600 processing queries whose only job is completed, so every one of them
	// is eligible for completion. A serial pass would finish all 600; two
	// contending passes without SKIP LOCKED would each try to take 500 of the
	// same rows.
	const total = 600
	for i := 0; i < total; i++ {
		var id int64
		if err := db.QueryRow(
			`INSERT INTO queries (text, text_hash, status) VALUES ($1, $2, 'processing') RETURNING id`,
			fmt.Sprintf("conc-%d", i), fmt.Sprintf("conc-%d", i)).Scan(&id); err != nil {
			t.Fatalf("seed query %d: %v", i, err)
		}
		if _, err := db.Exec(
			`INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, engine, status, result_count)
			 VALUES ($1, $2, 'http://example/', 0, 'bing', 'completed', 1)`,
			fmt.Sprintf("conc-job-%d", i), id); err != nil {
			t.Fatalf("seed job %d: %v", i, err)
		}
	}

	type outcome struct {
		res ReconcileResult
		err error
		d   time.Duration
	}
	start := make(chan struct{})
	out := make(chan outcome, 2)
	for i := 0; i < 2; i++ {
		go func() {
			<-start // release both goroutines at once
			t0 := time.Now()
			r, err := ReconcileProcessingQueries(context.Background(), db, []string{"bing", "duckduckgo"})
			out <- outcome{r, err, time.Since(t0)}
		}()
	}
	close(start)

	var sum int
	for i := 0; i < 2; i++ {
		o := <-out
		if o.err != nil {
			t.Fatalf("concurrent reconcile errored: %v", o.err)
		}
		t.Logf("caller finished in %s: %+v", o.d.Round(time.Millisecond), o.res)
		if o.d > 4*time.Second {
			t.Errorf("caller took %s — looks like it blocked on the other caller", o.d)
		}
		sum += o.res.Completed
	}
	if sum != total {
		t.Errorf("total completed = %d, want %d (rows were either taken twice or skipped)", sum, total)
	}

	var stillProcessing int
	if err := db.QueryRow(`SELECT COUNT(*) FROM queries WHERE status = 'processing'`).Scan(&stillProcessing); err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if stillProcessing != 0 {
		t.Errorf("%d queries still processing after two concurrent passes, want 0", stillProcessing)
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

// startReconcilePostgres boots a throwaway Postgres bound to loopback with
// random credentials and registers its own cleanup. The hook in this
// environment blocks docker rm from an ad-hoc shell, so the removal has to
// happen inside the test process.
func startReconcilePostgres(t *testing.T) *sql.DB {
	t.Helper()

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

	// Credentials are random and may contain URL-reserved characters, so
	// build the DSN through net/url rather than Sprintf.
	u := &url.URL{
		Scheme:   "postgres",
		User:     url.UserPassword(user, pass),
		Host:     "127.0.0.1:" + port,
		Path:     "/xxchk",
		RawQuery: "sslmode=disable",
	}
	return waitForDB(t, u.String())
}

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
