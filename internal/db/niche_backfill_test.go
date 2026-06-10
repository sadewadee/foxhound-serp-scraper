package db

// Tests for the niche lineage backfill (BackfillListingNicheInherit). Validate,
// WITHOUT a live DB, that the niche buckets + precedence applied to QUERY TEXT
// reproduce the trigger's classification — and that bucket-less PersonalNiches
// stay NULL (the rows we intentionally leave for a product decision).

import (
	"regexp"
	"strings"
	"testing"
)

// classifyNicheGo mirrors the SQL CASE (buildNicheCaseSQL) in Go: it walks
// nicheBuckets in order, first match wins, translating Postgres word boundaries
// \m/\M to Go's \b. It exercises the exact patterns + precedence the backfill
// ships, so a typo or reordering in nicheBuckets fails here.
func classifyNicheGo(text string) string {
	low := strings.ToLower(text)
	for _, nb := range nicheBuckets {
		goPat := strings.ReplaceAll(nb.pattern, `\m`, `\b`)
		goPat = strings.ReplaceAll(goPat, `\M`, `\b`)
		if regexp.MustCompile(`(?i)` + goPat).MatchString(low) {
			return nb.bucket
		}
	}
	return ""
}

func TestNicheInherit_ClassifiesQueryText(t *testing.T) {
	cases := []struct {
		text string
		want string
	}{
		// Business niche queries ("<keyword> <city> <operator>").
		{"yoga studio jakarta contact", "yoga"},
		{"vinyasa yoga bali \"@gmail.com\"", "yoga"},
		{"reformer pilates singapore email", "pilates"},
		{"crossfit box austin contact", "fitness"},
		{"premier gym membership london", "fitness"},
		{"meditation center dublin contact", "meditation"},
		{"sound healing retreat ubud", "healing"},
		{"thai massage day spa phuket", "spa"},
		{"holistic wellness center sedona", "wellness"},
		// Precedence: yoga checked before everything else.
		{"yoga and pilates studio bali", "yoga"},
		// PersonalNiches with no bucket → NULL (left for the product decision).
		{"hypnotherapist london contact", ""},
		{"personal trainer manchester email", ""},
		{"health coach sydney \"@gmail.com\"", ""},
		{"life coach toronto contact", ""},
		// Defensive: pure city/operator with no niche resolves to NULL.
		{"contact email jakarta", ""},
	}
	for _, c := range cases {
		if got := classifyNicheGo(c.text); got != c.want {
			t.Errorf("classify(%q) = %q, want %q", c.text, got, c.want)
		}
	}
}

func TestNicheInherit_AyurvedaPrefixMatches(t *testing.T) {
	// The \m-prefix bucket (no trailing \M) matches both "ayurveda" and
	// "ayurvedic". MUST stay in lockstep with the trigger CASE in migrate.go —
	// if someone reverts to `\mayurved\M` here, fix the trigger in the same change.
	for _, s := range []string{"ayurveda clinic kerala", "ayurvedic center bali", "ayurvedic practitioner sydney"} {
		if got := classifyNicheGo(s); got != "ayurveda" {
			t.Errorf("classify(%q) = %q, want ayurveda", s, got)
		}
	}
}

func TestBuildNicheCaseSQL_ShapeAndCoverage(t *testing.T) {
	sql := buildNicheCaseSQL("LOWER(q.text)")
	if !strings.HasPrefix(sql, "CASE") || !strings.HasSuffix(sql, "ELSE NULL END") {
		t.Fatalf("unexpected CASE shape: %q", sql)
	}
	// Every distinct bucket label must appear as a THEN target.
	for _, want := range []string{"yoga", "pilates", "fitness", "meditation", "healing", "ayurveda", "spa", "wellness"} {
		if !strings.Contains(sql, "THEN '"+want+"'") {
			t.Errorf("CASE missing bucket %q: %s", want, sql)
		}
	}
	// The text expression is interpolated, not hardcoded to a column name.
	if !strings.Contains(sql, "LOWER(q.text) ~ ") {
		t.Errorf("CASE did not interpolate the text expression: %s", sql)
	}
}
