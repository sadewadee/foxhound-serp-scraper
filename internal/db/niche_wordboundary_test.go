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
		{"hatha yoga", "yoga"},
		// "yogi"/"yogis" were REMOVED from the yoga group in the 2026-09-26
		// false-positive audit fix — "Yogi Flight School" (Organization) had
		// no yoga content at all, just a founder/brand name containing
		// "yogi". "yoga" (the whole word) still matches; "yogi" alone no
		// longer buckets anything.
		{"yogi tea house", ""},
		{"yogi flight school", ""},
		// "spin" (bare) was REPLACED with specific fitness phrases in the
		// same fix — "Hero Spin Casino" and "free spins" (casino/slots
		// content) must NOT bucket as fitness, while real spin-class copy
		// still must.
		{"hero spin casino", ""},
		{"free spins", ""},
		{"spin class", "fitness"},
		{"indoor cycling studio", "fitness"},
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

// TestTriggerHardOffNicheListMatchesGoSlice is the lockstep guard between
// hardOffNicheTypes (niche.go) and the trg_normalize_enrichment off_niche
// CASE's HARD off-niche @type IN(...) list (migrate.go) — the 2026-09-26
// false-positive audit fix that made hard @type values (Hotel, Casino, ...)
// win over keyword evidence again. Unlike buildReclassifyPredicateSQL (which
// renders hardOffNicheTypes directly into SQL via sqlQuotedList), the
// trigger's list is a hand-maintained SQL literal — replaceTriggerFunctionIfNewer
// / fmt.Sprintf render trgNormalizeEnrichmentFnBody with only the version
// marker substituted (see TestTriggerFnBodiesEmbedVersionMarker in
// trigger_version_guard_test.go), so nothing else catches the two lists
// drifting apart. Reads migrate.go via os.ReadFile and asserts every
// hardOffNicheTypes entry appears, in order, inside the specific IN(...)
// block marked with the "lockstep: hardOffNicheTypes (niche.go)" comment.
func TestTriggerHardOffNicheListMatchesGoSlice(t *testing.T) {
	src, err := os.ReadFile("migrate.go")
	if err != nil {
		t.Fatalf("reading migrate.go: %v", err)
	}
	content := string(src)

	const startMarker = "lockstep: hardOffNicheTypes (niche.go)"
	startIdx := strings.Index(content, startMarker)
	if startIdx == -1 {
		t.Fatal("could not find the hardOffNicheTypes lockstep marker in migrate.go's off_niche CASE — has the trigger been restructured? update this test's markers (and the marker comment itself) to match")
	}

	const endMarker = ") THEN TRUE"
	relEndIdx := strings.Index(content[startIdx:], endMarker)
	if relEndIdx == -1 {
		t.Fatal("could not find the closing `) THEN TRUE` for the hard off-niche @type IN(...) list in migrate.go")
	}
	block := content[startIdx : startIdx+relEndIdx+len(endMarker)]

	cursor := 0
	for _, want := range hardOffNicheTypes {
		wantQuoted := "'" + want + "'"
		idx := strings.Index(block[cursor:], wantQuoted)
		if idx == -1 {
			t.Errorf("trigger hard off-niche @type IN(...) list missing (or out of order relative to hardOffNicheTypes): %s", wantQuoted)
			continue
		}
		cursor += idx + len(wantQuoted)
	}
}

// TestOffNicheTypeListsAreSQLSafe guards sqlQuotedList's safety assumption
// (niche.go doc comment): direct '...' interpolation of hardOffNicheTypes /
// contentCommerceOffNicheTypes is only safe because neither slice's entries
// contain a quote (which would break out of the SQL string literal) or a
// '%' (which fmt.Sprintf would otherwise treat as a directive in some
// render paths). A future entry violating either must fail here, not in
// production SQL.
func TestOffNicheTypeListsAreSQLSafe(t *testing.T) {
	for _, list := range [][]string{hardOffNicheTypes, contentCommerceOffNicheTypes} {
		for _, v := range list {
			if strings.ContainsAny(v, `'%`) {
				t.Errorf("off-niche @type entry %q contains a quote or '%%' — unsafe for direct SQL interpolation via sqlQuotedList", v)
			}
		}
	}
}
