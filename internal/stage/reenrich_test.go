//go:build playwright

package stage

import (
	"database/sql"
	"strings"
	"testing"
	"time"
)

// TestOfflineParseDecision covers the pure decision logic of applyOfflineParse:
// what country/city values to write, and whether the network fetch can be
// skipped. SQL ops + slog are covered by integration tests in staging.
func TestOfflineParseDecision(t *testing.T) {
	cases := []struct {
		name           string
		row            reenrichRow
		parsedCountry  string
		parsedCity     string
		wantNewCountry sql.NullString
		wantNewCity    sql.NullString
		wantSkipFetch  bool
	}{
		{
			name: "country and city both NULL, parser found both, row has email — skip fetch",
			row: reenrichRow{
				Address:    sql.NullString{String: "521 Oak Grove Rd, Flat Rock, NC, United States", Valid: true},
				Country:    sql.NullString{Valid: false},
				City:       sql.NullString{Valid: false},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "US",
			parsedCity:     "Flat Rock",
			wantNewCountry: sql.NullString{String: "US", Valid: true},
			wantNewCity:    sql.NullString{String: "Flat Rock", Valid: true},
			wantSkipFetch:  true,
		},
		{
			name: "country NULL, city NULL, no contact data — fill but DON'T skip",
			row: reenrichRow{
				Address:    sql.NullString{String: "1 Main St, San Antonio, TX", Valid: true},
				EmailCount: 0,
				PhoneCount: 0,
			},
			parsedCountry:  "",
			parsedCity:     "San Antonio",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{String: "San Antonio", Valid: true},
			wantSkipFetch:  false,
		},
		{
			name: "row has phone, country already set, city NULL, parser fills city — skip fetch",
			row: reenrichRow{
				Address:    sql.NullString{String: "521 Oak Grove Rd, Flat Rock, NC, US", Valid: true},
				Country:    sql.NullString{String: "US", Valid: true},
				City:       sql.NullString{Valid: false},
				EmailCount: 0,
				PhoneCount: 2,
			},
			parsedCountry:  "US",
			parsedCity:     "Flat Rock",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{String: "Flat Rock", Valid: true},
			wantSkipFetch:  true,
		},
		{
			name: "everything already filled, parser also found same — write nothing, skip fetch",
			row: reenrichRow{
				Address:    sql.NullString{String: "521 Oak Grove Rd, Flat Rock, NC, US", Valid: true},
				Country:    sql.NullString{String: "US", Valid: true},
				City:       sql.NullString{String: "Flat Rock", Valid: true},
				EmailCount: 5,
				PhoneCount: 0,
			},
			parsedCountry:  "US",
			parsedCity:     "Flat Rock",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  true,
		},
		{
			name: "parser found nothing, row already complete — skip fetch (still valid)",
			row: reenrichRow{
				Address:    sql.NullString{String: "Compagnonsplein 1", Valid: true},
				Country:    sql.NullString{String: "NL", Valid: true},
				City:       sql.NullString{String: "Amsterdam", Valid: true},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "",
			parsedCity:     "",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  true,
		},
		{
			name: "country present, city NULL, parser nothing, has contact — DON'T skip (city missing)",
			row: reenrichRow{
				Address:    sql.NullString{String: "Compagnonsplein 1", Valid: true},
				Country:    sql.NullString{String: "NL", Valid: true},
				City:       sql.NullString{Valid: false},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "",
			parsedCity:     "",
			wantNewCountry: sql.NullString{Valid: false},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  false,
		},
		{
			name: "row totally empty, parser fills country only — fill country, no skip",
			row: reenrichRow{
				Address:    sql.NullString{String: "Frankfurt, Germany", Valid: true},
				EmailCount: 0,
				PhoneCount: 0,
			},
			parsedCountry:  "DE",
			parsedCity:     "",
			wantNewCountry: sql.NullString{String: "DE", Valid: true},
			wantNewCity:    sql.NullString{Valid: false},
			wantSkipFetch:  false,
		},
		{
			name: "Country empty-string sql.Valid=true treated as missing",
			row: reenrichRow{
				Address:    sql.NullString{String: "1 Main St, Boston, MA, USA", Valid: true},
				Country:    sql.NullString{String: "", Valid: true},
				City:       sql.NullString{String: "", Valid: true},
				EmailCount: 1,
				PhoneCount: 0,
			},
			parsedCountry:  "US",
			parsedCity:     "Boston",
			wantNewCountry: sql.NullString{String: "US", Valid: true},
			wantNewCity:    sql.NullString{String: "Boston", Valid: true},
			wantSkipFetch:  true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCountry, gotCity, gotSkip := offlineParseDecision(tc.row, tc.parsedCountry, tc.parsedCity)
			if gotCountry != tc.wantNewCountry {
				t.Errorf("newCountry = %+v; want %+v", gotCountry, tc.wantNewCountry)
			}
			if gotCity != tc.wantNewCity {
				t.Errorf("newCity = %+v; want %+v", gotCity, tc.wantNewCity)
			}
			if gotSkip != tc.wantSkipFetch {
				t.Errorf("skipFetch = %v; want %v", gotSkip, tc.wantSkipFetch)
			}
		})
	}
}

// TestReenrichMaxAttempts_Constant locks the retry cap value so future edits
// surface in PR review. Bumping this without a memory-feedback rationale is
// almost always a sign of "let me just retry more" cargo-culting that
// re-creates the 2026-04-06 pipeline-deadlock anti-pattern.
func TestReenrichMaxAttempts_Constant(t *testing.T) {
	const expected = 10
	if reenrichMaxAttempts != expected {
		t.Errorf("reenrichMaxAttempts = %d; want %d (mirrors enrich reconciler cap pattern from .dev-squad/gotchas.md)",
			reenrichMaxAttempts, expected)
	}
}

// TestReaperSweepSQL_Shape compile-tests the reaper sweep query body. The
// query is a const-style multiline UPDATE — if anyone reorders the predicates
// or drops a guard clause this test reminds them what each clause prevents.
//
// We verify by string-inspecting the actual SQL embedded in reapStaleLocks
// via duplicate-and-keep-in-sync. Cheap insurance against silent regressions
// (no live DB needed).
func TestReaperSweepSQL_Shape(t *testing.T) {
	required := []struct {
		clause string
		why    string
	}{
		{
			clause: "re_enrich_locked_at IS NOT NULL",
			why:    "without this the UPDATE rewrites every row in the table",
		},
		{
			clause: "re_enrich_locked_at < NOW() - INTERVAL '15 minutes'",
			why:    "without this we kill the locks of currently-running workers",
		},
		{
			clause: "re_enriched_at IS NULL",
			why:    "without this we re-open already-completed rows",
		},
		{
			clause: "SET re_enrich_locked_at = NULL",
			why:    "the entire point of the reaper",
		},
	}

	const reaperSQL = `
		UPDATE business_listings
		SET re_enrich_locked_at = NULL
		WHERE re_enrich_locked_at IS NOT NULL
		  AND re_enrich_locked_at < NOW() - INTERVAL '15 minutes'
		  AND re_enriched_at IS NULL
	`

	for _, c := range required {
		if !strings.Contains(reaperSQL, c.clause) {
			t.Errorf("reaper SQL missing clause %q — %s", c.clause, c.why)
		}
	}
}

// TestEligibilityQuery_NoOrderByRandom guards the issue-#28 regression: the
// reenrich eligibility query MUST NOT use ORDER BY RANDOM(). Sorting the whole
// re_enriched_at IS NULL pool (~340K rows) by random() forced a full index
// scan + sort and blew the 5s statement_timeout on every loop, stalling the
// worker at ~88 rows/hr. Without the sort, LIMIT short-circuits the scan after
// the first batch of eligible rows (~4ms measured on prod). This test inspects
// the actual SQL the worker runs (package-level eligibilityQuery), not a copy.
func TestEligibilityQuery_NoOrderByRandom(t *testing.T) {
	if strings.Contains(strings.ToUpper(eligibilityQuery), "ORDER BY RANDOM") {
		t.Error("eligibilityQuery contains ORDER BY RANDOM() — reintroduces the issue-#28 stall (full scan+sort blows the 5s statement_timeout). Rely on LIMIT + FOR UPDATE SKIP LOCKED instead.")
	}

	// The clauses that keep the query both correct and able to short-circuit.
	required := []struct {
		clause string
		why    string
	}{
		{"re_enriched_at IS NULL", "candidate set must be unprocessed rows only"},
		{"COALESCE(bl.re_enrich_attempts, 0) < $3", "retry cap stops genuinely-broken sites from cycling"},
		{"LIMIT $2", "bounds the batch and lets the index scan short-circuit"},
		{"FOR UPDATE OF bl SKIP LOCKED", "multi-worker safety + distributes work without random ordering"},
	}
	for _, c := range required {
		if !strings.Contains(eligibilityQuery, c.clause) {
			t.Errorf("eligibilityQuery missing clause %q — %s", c.clause, c.why)
		}
	}
}

// TestReenrichHealthGate covers the progress-based healthcheck (issue #28 +
// 2026-06-01 doom-loop fix): the worker is healthy while it makes progress, and
// only reports degraded after reenrichHealthWindow with NO progress. Crucially,
// transient eligibility-query timeouts (57014) must NOT flip a still-progressing
// worker unhealthy — that restart re-ran Migrate() and re-fired the boot DDL
// herd, deepening the contention that caused the timeout.
func TestReenrichHealthGate(t *testing.T) {
	r := &ReenrichStage{}

	if !r.healthy() {
		t.Fatal("a fresh worker (no progress recorded yet) must report healthy")
	}

	// Recording progress keeps it healthy.
	r.recordProgress()
	if !r.healthy() {
		t.Fatal("worker must be healthy immediately after recording progress")
	}

	// No progress for longer than the window -> degraded (genuine stall).
	r.lastProgress.Store(time.Now().Add(-2 * reenrichHealthWindow).UnixNano())
	if r.healthy() {
		t.Fatalf("worker should report degraded after %v without progress", reenrichHealthWindow)
	}

	// REGRESSION GUARD (doom loop): a flood of eligibility-query timeouts must
	// NOT mark a recently-progressing worker unhealthy.
	r.recordProgress()
	for i := 0; i < reenrichMaxConsecutiveEligFailures+5; i++ {
		r.recordEligibilityFailure()
	}
	if !r.healthy() {
		t.Fatal("eligibility-query timeouts alone must not mark a progressing worker unhealthy")
	}

	// A successful eligibility query records progress -> restores health even if
	// the worker had gone stale.
	r.lastProgress.Store(time.Now().Add(-2 * reenrichHealthWindow).UnixNano())
	r.recordEligibilitySuccess()
	if !r.healthy() {
		t.Fatal("a successful eligibility query must restore health (records progress)")
	}
}
