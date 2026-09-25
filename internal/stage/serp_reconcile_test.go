package stage

import (
	"context"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/sadewadee/serp-scraper/internal/testpg"
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
		"engine retired globally (SERP_RETIRED_ENGINES)",
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

func TestParseRetiredEngines(t *testing.T) {
	tests := []struct {
		in   string
		want []string
	}{
		{"google", []string{"google"}},
		{"google,bing", []string{"google", "bing"}},
		{" Google , BING ", []string{"google", "bing"}},
		{"", nil},
		{",,", nil},
		{"google,,duckduckgo", []string{"google", "duckduckgo"}},
	}
	for _, tt := range tests {
		got := ParseRetiredEngines(tt.in)
		if len(got) != len(tt.want) {
			t.Errorf("ParseRetiredEngines(%q) = %v, want %v", tt.in, got, tt.want)
			continue
		}
		for i := range got {
			if got[i] != tt.want[i] {
				t.Errorf("ParseRetiredEngines(%q)[%d] = %q, want %q", tt.in, i, got[i], tt.want[i])
			}
		}
	}
}

// TestReconcileProcessingQueries_HeterogeneousHosts is the regression test for
// the production setup where hachibi (searxng,duckduckgo) and kurawa
// (bing,duckduckgo) share the serp_jobs tables. A host's engine set only says
// what that host can process — it never retires engines globally. So with the
// same retired list, BOTH hosts must reach the same verdicts: engines merely
// absent elsewhere are left alone, pending jobs of another host block
// completion, and only truly retired engines are dead-lettered.
func TestReconcileProcessingQueries_HeterogeneousHosts(t *testing.T) {
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_reconcile_"}).DB()
	defer db.Close()
	if _, err := db.Exec(reconcileTestSchema); err != nil {
		t.Fatalf("schema: %v", err)
	}

	seed := []string{"h-mixed", "h-all-terminal", "h-google", "h-zombie"}
	ids := map[string]int64{}
	for _, text := range seed {
		var id int64
		if err := db.QueryRow(
			`INSERT INTO queries (text, text_hash, status) VALUES ($1, $2, 'processing') RETURNING id`,
			text, text).Scan(&id); err != nil {
			t.Fatalf("seed query %s: %v", text, err)
		}
		ids[text] = id
	}
	jobs := []struct {
		query   string
		suffix  string
		engine  string
		status  string
		results int
	}{
		{"h-mixed", "a", "searxng", "new", 0}, // pending on the other host — must block
		{"h-mixed", "b", "bing", "completed", 4},
		{"h-all-terminal", "a", "bing", "completed", 4},
		{"h-all-terminal", "b", "duckduckgo", "completed", 2},
		{"h-google", "a", "google", "new", 0}, // retired — must die
		{"h-google", "b", "duckduckgo", "completed", 6},
	}
	for _, j := range jobs {
		if _, err := db.Exec(
			`INSERT INTO serp_jobs (id, parent_job_id, search_url, page_num, engine, status, result_count)
			 VALUES ($1, $2, 'http://example/', 0, $3, $4, $5)`,
			j.query+"-"+j.suffix, ids[j.query], j.engine, j.status, j.results,
		); err != nil {
			t.Fatalf("seed job %s-%s: %v", j.query, j.suffix, err)
		}
	}

	retired := []string{"google"}

	// Both hosts pass the same retired list — never their own engine set, so
	// the reconcile outcome is host-agnostic by construction.
	resA, err := ReconcileProcessingQueries(context.Background(), db, retired)
	if err != nil {
		t.Fatalf("host A reconcile: %v", err)
	}
	// h-mixed stays processing (searxng job pending on hachibi), h-all-terminal
	// completes, h-google completes after its retired google job dies,
	// h-zombie is requeued.
	if resA.Completed != 2 || resA.Failed != 0 || resA.Requeued != 1 || resA.Active != 1 {
		t.Errorf("host A result = %+v; want completed=2 failed=0 requeued=1 active=1", resA)
	}
	want := map[string]string{
		"h-mixed":        "processing",
		"h-all-terminal": "completed",
		"h-google":       "completed",
		"h-zombie":       "pending",
	}
	for text, status := range want {
		var got string
		if err := db.QueryRow(`SELECT status FROM queries WHERE id = $1`, ids[text]).Scan(&got); err != nil {
			t.Fatalf("read %s: %v", text, err)
		}
		if got != status {
			t.Errorf("%s status = %q, want %q", text, got, status)
		}
	}

	// The searxng job must be untouched (status still 'new') — neither host
	// may see it as dead even though they run different engine sets.
	var sxStatus, sxErr string
	if err := db.QueryRow(
		`SELECT status, COALESCE(error_msg,'') FROM serp_jobs WHERE id = 'h-mixed-a'`).Scan(&sxStatus, &sxErr); err != nil {
		t.Fatalf("read searxng job: %v", err)
	}
	if sxStatus != "new" || sxErr != "" {
		t.Errorf("searxng job = %s/%q, want new/\"\" (must be untouched on both hosts)", sxStatus, sxErr)
	}
	// The bing job under h-mixed must also be untouched.
	var bingStatus string
	if err := db.QueryRow(`SELECT status FROM serp_jobs WHERE id = 'h-mixed-b'`).Scan(&bingStatus); err != nil {
		t.Fatalf("read bing job: %v", err)
	}
	if bingStatus != "completed" {
		t.Errorf("bing job under h-mixed = %q, want 'completed' (must be untouched)", bingStatus)
	}
	// The google job must be dead with the retired message.
	var gStatus, gErr string
	if err := db.QueryRow(
		`SELECT status, COALESCE(error_msg,'') FROM serp_jobs WHERE id = 'h-google-a'`).Scan(&gStatus, &gErr); err != nil {
		t.Fatalf("read google job: %v", err)
	}
	if gStatus != "dead" || gErr != "engine retired globally (SERP_RETIRED_ENGINES)" {
		t.Errorf("google job = %s/%q, want dead/'engine retired globally (SERP_RETIRED_ENGINES)'", gStatus, gErr)
	}

	// Host B (kurawa) runs a pass over h-mixed, whose only live work is a
	// searxng job that host B cannot serve. The query must stay processing and
	// the job must survive: a host's engine set says what that host can run,
	// never what is globally dead.
	if _, err := ReconcileProcessingQueries(context.Background(), db, retired); err != nil {
		t.Fatalf("host B reconcile: %v", err)
	}
	var mixedStatus string
	if err := db.QueryRow(`SELECT status FROM queries WHERE id = $1`, ids["h-mixed"]).Scan(&mixedStatus); err != nil {
		t.Fatalf("read h-mixed: %v", err)
	}
	if mixedStatus != "processing" {
		t.Errorf("h-mixed = %q after host B, want 'processing' (searxng job still pending)", mixedStatus)
	}
	if err := db.QueryRow(`SELECT status FROM serp_jobs WHERE id = 'h-mixed-a'`).Scan(&sxStatus); err != nil {
		t.Fatalf("read searxng job again: %v", err)
	}
	if sxStatus != "new" {
		t.Errorf("searxng job = %q after host B, want 'new' (must never be dead-lettered by kurawa)", sxStatus)
	}
}

// TestReconcileProcessingQueries_Integration seeds the five query states the
// reconciler must distinguish and runs one pass against a throwaway Postgres.
func TestReconcileProcessingQueries_Integration(t *testing.T) {

	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_reconcile_"}).DB()
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

	res, err := ReconcileProcessingQueries(context.Background(), db, []string{"google"})
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
	if googleStatus != "dead" || googleErr != "engine retired globally (SERP_RETIRED_ENGINES)" {
		t.Errorf("retired-engine job = %s/%q, want dead/'engine retired globally (SERP_RETIRED_ENGINES)'", googleStatus, googleErr)
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
	db := testpg.Start(t, testpg.StartOpts{Prefix: "xxchk_reconcile_"}).DB()
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
			r, err := ReconcileProcessingQueries(context.Background(), db, []string{"google"})
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
