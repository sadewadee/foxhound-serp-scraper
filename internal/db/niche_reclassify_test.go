package db

// Tests for ReclassifyOffNicheByKeyword's pure-function pieces (SQL builders
// + dry-run env gate) — no live DB required. Mirrors the style of
// niche_backfill_test.go / migrate_niche_test.go: assert shape/precedence
// via string content, not execution.

import (
	"strings"
	"testing"
)

func TestNicheReclassifyDryRunEnabled_DefaultsTrue(t *testing.T) {
	cases := []struct {
		name string
		val  string // t.Setenv value; "" is behaviorally identical to unset for this function
		want bool
	}{
		{"unset/empty defaults to dry-run (safe)", "", true},
		{"explicit false disables dry-run (write mode)", "false", false},
		{"explicit FALSE (case-insensitive) disables dry-run", "FALSE", false},
		{"surrounding whitespace still recognized", "  false  ", false},
		{"explicit true stays dry-run", "true", true},
		{"explicit 1 stays dry-run — only the literal 'false' opts in", "1", true},
		{"garbage value stays dry-run (safe default)", "nah", true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("NICHE_RECLASSIFY_DRY_RUN", tc.val)
			if got := nicheReclassifyDryRunEnabled(); got != tc.want {
				t.Errorf("nicheReclassifyDryRunEnabled() = %v; want %v", got, tc.want)
			}
		})
	}
}

func TestBuildReclassifyPredicateSQL_Shape(t *testing.T) {
	sql := buildReclassifyPredicateSQL()
	for _, want := range []string{
		"off_niche = TRUE",
		reclassifyLowerTextExpr,
		beautyOffNichePattern,
		"IS NOT NULL",
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("buildReclassifyPredicateSQL() missing %q: %s", want, sql)
		}
	}
	// The beauty pattern must be negated (!~), never used as a positive match —
	// beauty rows must stay excluded, not get reclassified.
	if !strings.Contains(sql, "!~ '"+beautyOffNichePattern+"'") {
		t.Errorf("buildReclassifyPredicateSQL() must negate the beauty pattern with !~: %s", sql)
	}
}

func TestBuildReclassifyCountSQL_Shape(t *testing.T) {
	sql := buildReclassifyCountSQL()
	if !strings.HasPrefix(sql, "SELECT COUNT(*) FROM business_listings WHERE id > $1 AND id <= $2 AND ") {
		t.Fatalf("unexpected count SQL shape: %s", sql)
	}
	if !strings.Contains(sql, buildReclassifyPredicateSQL()) {
		t.Errorf("count SQL predicate diverges from buildReclassifyPredicateSQL(): %s", sql)
	}
}

func TestBuildReclassifyBackupInsertSQL_Shape(t *testing.T) {
	sql := buildReclassifyBackupInsertSQL()
	for _, want := range []string{
		"INSERT INTO business_listings_offniche_backup_20260925",
		"id > $1 AND id <= $2",
		buildReclassifyPredicateSQL(),
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("buildReclassifyBackupInsertSQL() missing %q: %s", want, sql)
		}
	}
}

func TestBuildReclassifyUpdateSQL_Shape(t *testing.T) {
	sql := buildReclassifyUpdateSQL()
	for _, want := range []string{
		"UPDATE business_listings",
		"SET off_niche = FALSE",
		"query_inference",
		"updated_at = NOW()",
		"id > $1 AND id <= $2",
		buildReclassifyPredicateSQL(),
	} {
		if !strings.Contains(sql, want) {
			t.Errorf("buildReclassifyUpdateSQL() missing %q: %s", want, sql)
		}
	}
	// niche_category is only overwritten when currently empty or still
	// query-inferred — a page-extracted bucket from a later re-enrich must
	// never be clobbered by this one-time sweep.
	if !strings.Contains(sql, "WHEN niche_category IS NULL OR niche_source = 'query_inference'") {
		t.Errorf("buildReclassifyUpdateSQL() must guard niche_category overwrite: %s", sql)
	}
}

// TestReclassifyPredicate_NeverTouchesBeautyOrNoEvidence documents the two
// row classes ReclassifyOffNicheByKeyword must NEVER change, at the
// buildNicheCaseSQL/beautyOffNichePattern level shared with the trigger and
// niche.go's own classifyNicheGo-style translation (see
// niche_backfill_test.go): beauty-pattern rows, and rows with zero niche
// keyword evidence.
func TestReclassifyPredicate_NeverTouchesBeautyOrNoEvidence(t *testing.T) {
	predicate := buildReclassifyPredicateSQL()
	// Structural guard: the predicate must reference both the negated beauty
	// pattern and a positive (IS NOT NULL) niche-bucket requirement — if
	// either clause is dropped, the sweep could reclassify beauty rows or
	// rows with no keyword evidence at all.
	if strings.Count(predicate, "!~") != 1 {
		t.Errorf("predicate must negate exactly one pattern (beauty): %s", predicate)
	}
	if !strings.Contains(predicate, "CASE") {
		t.Errorf("predicate must embed the niche CASE (buildNicheCaseSQL) to require bucket evidence: %s", predicate)
	}
}
