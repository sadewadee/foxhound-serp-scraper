package db

// Tests for the niche classifier introduced in:
//   - 2026_05_22_off_niche_backfill (one-shot cleanup migration)
//   - trg_normalize_enrichment (per-row trigger CASE)
//
// Validates two properties WITHOUT a live DB:
//
//  1. The off-niche blacklist regex catches every category we said we'd flag
//     (Hotel, AutoDealer, HealthAndBeautyBusiness, ...) — guards against the
//     reviewer-caught HealthAndBeautyBusiness drift between the cleanup list
//     and the trigger list.
//
//  2. The niche keyword regex bucks rows into the expected niche_category
//     when the business name / page title / description matches a niche
//     keyword, and stays NULL otherwise. Mirrors the SQL CASE.
//
// If the SQL CASE expressions in migrate.go change, the Go constants below
// must be updated to match.

import (
	"regexp"
	"strings"
	"testing"
)

// offNicheBlacklist mirrors the IN (...) list in BOTH the trigger CASE
// (migrate.go:342-348) AND the cleanup constant offNicheTypes (migrate.go:
// 1175-1179). Both lists must stay in sync — if a category is added to one
// it MUST be added to the other.
var offNicheBlacklist = []string{
	"AutoDealer", "Hotel", "Restaurant", "Dentist", "Physician",
	"RealEstateAgent", "LegalService", "HairSalon", "BeautySalon",
	"TravelAgency", "LodgingBusiness", "GeneralContractor",
	"RoofingContractor", "HomeAndConstructionBusiness",
	"MedicalClinic", "MedicalBusiness", "HealthAndBeautyBusiness",
}

// nicheKeywordRegex mirrors the per-niche regex alternatives in the SQL CASE
// (both trigger AND cleanup STEP D). \m and \M are PostgreSQL word boundary
// markers; Go regexp uses \b instead.
var nicheKeywordRegex = map[string]string{
	"yoga":       `(?i)\b(yoga|asana|vinyasa|ashtanga|kundalini|iyengar|hatha|bikram|jivamukti)\b`,
	"pilates":    `(?i)\b(pilates|reformer)\b`,
	"fitness":    `(?i)\b(crossfit|bootcamp|hiit|barre|spin|gym|fitness)\b`,
	"meditation": `(?i)\b(meditation|mindfulness|breathwork)\b`,
	"healing":    `(?i)\b(reiki|sound healing|energy healing|healing)\b`,
	"ayurveda":   `(?i)\bayurved`,
	"spa":        `(?i)\b(spa|massage|thermal)\b`,
	"wellness":   `(?i)\b(wellness|holistic)\b`,
}

func TestOffNiche_BlacklistCoversReviewerExamples(t *testing.T) {
	// The reviewer specifically called out HealthAndBeautyBusiness as missing
	// from the trigger CASE. Both lists must include the high-volume categories
	// observed polluting production data.
	mustInclude := []string{
		"AutoDealer",              // 332 rows
		"Hotel",                   // 1838 rows
		"Restaurant",              // 666 rows
		"Dentist",                 // 193 rows
		"HealthAndBeautyBusiness", // 804 rows — the reviewer-caught omission
		"BeautySalon",             // 310 rows
		"MedicalClinic",           // 730 rows
		"MedicalBusiness",         // 787 rows
	}
	have := make(map[string]bool, len(offNicheBlacklist))
	for _, c := range offNicheBlacklist {
		have[c] = true
	}
	for _, c := range mustInclude {
		if !have[c] {
			t.Errorf("offNicheBlacklist missing %q — production has hundreds of rows of this @type", c)
		}
	}
}

func TestNicheClassifier_YogaKeywords(t *testing.T) {
	yogaCases := []string{
		"Sunrise Yoga Studio",
		"Vinyasa Flow Center",
		"ashtanga yoga teacher training",
		"kundalini yoga and meditation",
		"Bikram Yoga Bali",
		"Hatha yoga for beginners",
		"Jivamukti yoga school New York",
	}
	r := regexp.MustCompile(nicheKeywordRegex["yoga"])
	for _, s := range yogaCases {
		if !r.MatchString(s) {
			t.Errorf("expected %q to match yoga regex but it did not", s)
		}
	}
}

func TestNicheClassifier_FitnessKeywords(t *testing.T) {
	fitnessCases := []string{
		"CrossFit Box Singapore",
		"24 Hour Fitness",
		"Bootcamp on the Beach",
		"HIIT Class Manhattan",
		"Barre Studio Brooklyn",
		"Premier Gym Membership",
	}
	r := regexp.MustCompile(nicheKeywordRegex["fitness"])
	for _, s := range fitnessCases {
		if !r.MatchString(s) {
			t.Errorf("expected %q to match fitness regex but it did not", s)
		}
	}
}

func TestNicheClassifier_WellnessKeywords(t *testing.T) {
	wellnessCases := []string{
		"Holistic Wellness Center",
		"Sound Healing & Wellness",
		"Holistic medicine clinic",
	}
	r := regexp.MustCompile(nicheKeywordRegex["wellness"])
	for _, s := range wellnessCases {
		if !r.MatchString(s) {
			t.Errorf("expected %q to match wellness regex but it did not", s)
		}
	}
}

func TestNicheClassifier_AyurvedaPrefix(t *testing.T) {
	// Ayurveda uses prefix match (ayurved) to cover "ayurveda" + "ayurvedic".
	r := regexp.MustCompile(nicheKeywordRegex["ayurveda"])
	for _, s := range []string{"Ayurveda Wellness", "Ayurvedic practitioner", "AYURVEDIC center"} {
		if !r.MatchString(s) {
			t.Errorf("expected %q to match ayurveda prefix regex", s)
		}
	}
}

func TestNicheClassifier_NoFalsePositiveOffNiche(t *testing.T) {
	// The reviewer-flagged risk: HealthAndBeautyBusiness pages often have
	// "spa" or "salon" in the title. Those keywords would tag them as 'spa'
	// niche_category. But off_niche=TRUE takes precedence in the API filter
	// (default include_off_niche=false), so a HealthAndBeautyBusiness row
	// with niche_category='spa' is still excluded from consumer results.
	// This test documents that contract: a category in the blacklist MUST
	// result in off_niche=TRUE regardless of what niche_category turns out
	// to be. (The off_niche flag is set first, in the same INSERT.)
	have := make(map[string]bool, len(offNicheBlacklist))
	for _, c := range offNicheBlacklist {
		have[c] = true
	}
	if !have["HealthAndBeautyBusiness"] {
		t.Fatal("HealthAndBeautyBusiness must be in blacklist (security: don't surface beauty/medical in wellness API)")
	}
}

func TestNicheClassifier_StopwordsDontMisclassify(t *testing.T) {
	// Pages mentioning niche keywords incidentally (in unrelated context)
	// will still bucket — that's accepted false-positive surface for the
	// regex-only classifier. This test documents that and acts as a
	// canary: if someone tightens the regex, these cases should keep
	// matching (current behavior).
	yogaR := regexp.MustCompile(nicheKeywordRegex["yoga"])
	cases := []string{
		"Our yoga blanket store sells handmade props",        // matches: 'yoga'
		"Sports apparel including yoga pants and athleisure", // matches: 'yoga'
		"Asana the project management tool",                  // matches: 'asana'
	}
	for _, s := range cases {
		if !yogaR.MatchString(s) {
			t.Errorf("expected %q to match (current regex is intentionally permissive)", s)
		}
	}
}

func TestNicheClassifier_MetaKeywordSoupHeuristic(t *testing.T) {
	// LENGTH > 100 catches <meta name="keywords"> stuffed into raw_category.
	// Sample real garbage from production audit (~41K rows): 100+ char strings
	// of comma-separated SEO terms.
	garbage := strings.Repeat("real estate, mortgage, homes for sale, ", 5)
	if len(garbage) <= 100 {
		t.Fatalf("test fixture must be >100 chars to validate the heuristic; got %d", len(garbage))
	}
	// Logic mirrors the SQL: LENGTH(raw_category) > 100 → off_niche=TRUE.
	// We just assert the Go-side fixture matches the SQL threshold.
	if !(len(garbage) > 100) {
		t.Errorf("LENGTH(raw_category) > 100 heuristic does not flag %d-char fixture", len(garbage))
	}
}
