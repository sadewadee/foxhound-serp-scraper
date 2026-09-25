package db

// Regression guard for the 2026-09-25 regex-precedence fix: an un-grouped
// Postgres ARE alternation (`\ma|b|c\M`) only anchors the FIRST alternative
// at word-start and the LAST at word-end — every alternative in between
// matches as an unanchored substring ("spin" inside "spinach", "barre"
// inside "barrel"). nicheBuckets in niche.go was wrapped `\m(a|b|c)\M` to
// fix this; TestNicheClassifier_* in migrate_niche_test.go only exercises a
// SEPARATE, already-correct hardcoded Go map (nicheKeywordRegex), so it
// never would have caught a regression in the REAL nicheBuckets patterns.
// This file closes that gap by testing nicheBuckets directly.
//
// classifyNicheGo (the \m/\M -> \b conversion + first-match-wins loop) is
// already defined in niche_backfill_test.go, same package — reused here
// rather than duplicated.

import (
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestNicheBuckets_WordBoundaries(t *testing.T) {
	cases := []struct {
		text string
		want string
	}{
		// The exact false-positive class the un-grouped pattern produced:
		// "spin" (fitness) must NOT match as a bare substring of "spinach".
		{"spinach smoothie bar", ""},
		// "barre" (fitness) must NOT match as a bare substring of "barrel".
		// "sauna" has no bucket of its own, so the overall result is "",
		// not some other accidental bucket.
		{"barrel sauna", ""},
		// Real fitness keywords as WHOLE words must still match correctly —
		// the fix must not have overcorrected into under-matching.
		{"hiit class", "fitness"},
		{"barre studio", "fitness"},
		{"spin class", "fitness"},
		{"hatha yoga", "yoga"},
		// "yogi" is an explicit inflection added alongside the grouping fix
		// (yoga|...|yogi|yogis) — intentional, documented here so a future
		// tightening of the yoga pattern doesn't silently drop it.
		{"yogi tea house", "yoga"},
		// "healing" (last alternative in the healing group) as a whole word
		// must match at both string-boundary positions (after a leading
		// word, and at end of string).
		{"healing touch", "healing"},
		{"faith healing", "healing"},
		// "healingly" must NOT match — "healing" only satisfies the leading
		// \m boundary here, not a trailing \M (the group requires a word
		// boundary immediately after "healing", but "ly" continues the word).
		{"healingly", ""},
		// Prefix-only stem groups (bodywork, ayurveda) are INTENTIONALLY
		// \m-only (no trailing \M) so they match inflections — this must
		// keep working after the grouping fix.
		{"osteopathy clinic", "bodywork"},
		// Precedence guard: ayurveda (bucket 8) is checked before spa
		// (bucket 9) — "ayurvedic massage" contains both an ayurveda-prefix
		// match AND a standalone "massage" word; ayurveda must win because
		// it comes first in nicheBuckets, not because spa doesn't match.
		{"ayurvedic massage", "ayurveda"},
		{"day spa", "spa"},
		// "spa" (spa bucket, fully grouped \m(...)\M) must NOT match as a
		// bare prefix of "spaghetti" — the trailing \M boundary must hold.
		{"spaghetti house", ""},
	}
	for _, tc := range cases {
		t.Run(tc.text, func(t *testing.T) {
			if got := classifyNicheGo(tc.text); got != tc.want {
				t.Errorf("classifyNicheGo(%q) = %q; want %q", tc.text, got, tc.want)
			}
		})
	}
}

// TestTriggerNicheCaseMatchesNicheBuckets is the lockstep guard between the
// Go-side nicheBuckets slice (niche.go) and the actual SQL text of the
// trg_normalize_enrichment trigger's `niche_bucket := CASE` block
// (migrate.go) — the two are hand-maintained in parallel (no shared code
// generation), so nothing else catches one drifting from the other. Reads
// migrate.go via os.ReadFile (there is no package-level const/var exposing
// the trigger body as a string — it's an inline argument to db.Exec inside
// runMigrations) and asserts every nicheBuckets pattern+bucket appears in
// the CASE block, IN ORDER, as `niche_text ~ '<pattern>' THEN '<bucket>'`.
//
// No equivalent lockstep test existed before this one: migrate_niche_test.go
// only cross-checks a separately-hardcoded Go regex map
// (nicheKeywordRegex) and a separately-hardcoded off-niche blacklist slice
// against fixed literal examples — neither reads migrate.go's source, and
// neither iterates nicheBuckets itself. niche_backfill_test.go's
// classifyNicheGo DOES read nicheBuckets, but only exercises it against
// Go's regexp engine — it does not verify the SQL trigger text says the
// same thing.
func TestTriggerNicheCaseMatchesNicheBuckets(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("reading migrate.go: %v", err)
	}
	content := string(src)

	const startMarker = "niche_bucket := CASE"
	startIdx := strings.Index(content, startMarker)
	if startIdx == -1 {
		t.Fatal("could not find `niche_bucket := CASE` in migrate.go — has the trigger been restructured? update this test's markers to match")
	}

	const endMarker = "END;"
	relEndIdx := strings.Index(content[startIdx:], endMarker)
	if relEndIdx == -1 {
		t.Fatal("could not find the closing `END;` for the niche_bucket CASE in migrate.go")
	}
	block := content[startIdx : startIdx+relEndIdx+len(endMarker)]

	cursor := 0
	for _, nb := range nicheBuckets {
		want := fmt.Sprintf("niche_text ~ '%s' THEN '%s'", nb.pattern, nb.bucket)
		idx := strings.Index(block[cursor:], want)
		if idx == -1 {
			t.Errorf("trigger niche_bucket CASE missing (or out of order relative to nicheBuckets): %s", want)
			continue
		}
		cursor += idx + len(want)
	}
}
