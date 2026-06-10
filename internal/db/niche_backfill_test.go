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
		// v0.9.8 broad-wellness buckets (health-adjacent + broadened fitness).
		{"osteopath clinic berlin contact", "bodywork"},
		{"physiotherapist san diego instagram", "bodywork"},
		{"acupuncturist mombasa email", "bodywork"},
		{"craniosacral therapist bucharest reviews", "bodywork"},
		{"hypnotherapist london contact", "therapy"},
		{"psychotherapist dublin email", "therapy"},
		{"dietitian antalya instagram email", "nutrition"},
		{"nutritionist sydney \"@gmail.com\"", "nutrition"},
		{"naturopath vancouver contact", "naturopathy"},
		{"herbalist fremantle best rated", "naturopathy"},
		{"homeopath incheon reviews", "naturopathy"},
		{"life coach toronto contact", "coaching"},
		{"health coach sydney \"@gmail.com\"", "coaching"},
		{"personal trainer manchester email", "fitness"},
		{"conditioning coach palawan", "fitness"},
		{"kickboxing instructor lodz", "fitness"},
		{"zumba instructor belfast instagram", "fitness"},
		// Genuinely bucket-less long tail still resolves to NULL.
		{"doula portland contact", ""},
		{"midwife galway email", ""},
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

func TestBeautyOffNichePattern(t *testing.T) {
	// Translate Postgres \m → Go \b for the test (mirror of the SQL ~ usage).
	goPat := `(?i)` + strings.ReplaceAll(beautyOffNichePattern, `\m`, `\b`)
	r := regexp.MustCompile(goPat)
	offTarget := []string{
		"nail salon in fort lauderdale reviews",
		"esthetician the hague contact classes",
		"barbershop near sydney contact",
		"hairdresser glasgow email",
		"makeup artist dubai instagram",
		"microblading studio austin",
		"tattoo parlour berlin",
	}
	for _, s := range offTarget {
		if !r.MatchString(strings.ToLower(s)) {
			t.Errorf("expected %q to match beauty off-niche pattern", s)
		}
		// Off-target rows must NOT land in a niche bucket (they get off_niche'd).
		if got := classifyNicheGo(s); got != "" {
			t.Errorf("beauty %q unexpectedly bucketed as %q", s, got)
		}
	}
	// In-niche must NOT trip the beauty filter.
	for _, s := range []string{"yoga studio jakarta", "naturopath vancouver", "day spa phuket"} {
		if r.MatchString(strings.ToLower(s)) {
			t.Errorf("in-niche %q wrongly matched beauty pattern", s)
		}
	}
}

func TestBuildNicheCaseSQL_ShapeAndCoverage(t *testing.T) {
	sql := buildNicheCaseSQL("LOWER(q.text)")
	if !strings.HasPrefix(sql, "CASE") || !strings.HasSuffix(sql, "ELSE NULL END") {
		t.Fatalf("unexpected CASE shape: %q", sql)
	}
	// Every distinct bucket label must appear as a THEN target.
	for _, want := range []string{"yoga", "pilates", "fitness", "meditation", "healing", "ayurveda", "spa", "wellness", "bodywork", "therapy", "nutrition", "naturopathy", "coaching"} {
		if !strings.Contains(sql, "THEN '"+want+"'") {
			t.Errorf("CASE missing bucket %q: %s", want, sql)
		}
	}
	// The text expression is interpolated, not hardcoded to a column name.
	if !strings.Contains(sql, "LOWER(q.text) ~ ") {
		t.Errorf("CASE did not interpolate the text expression: %s", sql)
	}
}
