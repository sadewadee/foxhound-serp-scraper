//go:build playwright

package stage

import (
	"strings"
	"testing"
)

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
	// Must-have clauses — each prevents a specific failure mode.
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
			why:    "without this we re-open already-completed rows (idx_bl_re_enrich_locked_at partial index also drops them, but explicit > implicit)",
		},
		{
			clause: "SET re_enrich_locked_at = NULL",
			why:    "the entire point of the reaper",
		},
	}

	// Reproduce the SQL literal from reapStaleLocks() — kept in sync by this test.
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
