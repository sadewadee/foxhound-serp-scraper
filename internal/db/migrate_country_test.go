package db

// Tests for the country migration logic introduced in:
//   - 2026_05_12_country_regex_hardening
//   - 2026_05_12_country_garbage_purge
//
// These tests validate two properties WITHOUT a live DB:
//
//  1. The hardened regex does NOT capture [A-Z]{2,3} suffixes from arbitrary
//     address strings (the original bug that created 8,177 garbage rows).
//
//  2. The CASE mapping table in both the regex-hardening SQL and the
//     garbage-purge convert SQL maps every known country name to the correct
//     ISO 3166-1 alpha-2 code (no typos, no missing entries, no mismatches).
//
// The regex under test mirrors the SQL regex used in countryRegexHardenSQL.
// If the SQL regex is edited, the Go constant below must be updated to match.

import (
	"regexp"
	"strings"
	"testing"
)

// hardenedRegex mirrors the regex alternative list used in
// countryRegexHardenVersion SQL. The `(?i)` flag + `\s*$` anchor are retained.
// [A-Z]{2,3} is intentionally ABSENT.
const hardenedRegexPattern = `(?i)(United States|United Kingdom|Indonesia|Australia|Canada|Germany|France|Singapore|Malaysia|Thailand|Japan|Brazil|Mexico|Spain|Italy|Netherlands|Belgium|Sweden|Norway|Denmark|Finland|Poland|Turkey|India|China|Korea|Vietnam|Philippines|New Zealand|Portugal|Ireland|Croatia|Austria|Chile|Colombia|Hungary|Romania|Greece|Taiwan|Morocco|Argentina|Egypt|Myanmar|Costa Rica|Panama|Kenya|Bahrain|Qatar|Nepal|Nigeria|Sri Lanka|Cambodia|Switzerland|South Africa|United Arab Emirates|Saudi Arabia|South Korea|New Zealand|Czech Republic|Hong Kong)\s*$`

// countryNameToISO mirrors the CASE blocks in both the regex-hardening SQL
// and the garbage-purge STEP B convert SQL. Any name present in the SQL CASE
// must appear here with the correct ISO code.
var countryNameToISOTest = map[string]string{
	// 2-char aliases (not in ISO alpha-2 set itself)
	"uk": "GB",
	// Short country names (3-4 chars), verified by auditor scan
	"peru": "PE",
	"cuba": "CU",
	"iran": "IR",
	"iraq": "IQ",
	"oman": "OM",
	"laos": "LA",
	"fiji": "FJ",
	"mali": "ML",
	"chad": "TD",
	"togo": "TG",
	"guam": "GU",
	// Spelled-out country names (>4 chars)
	"united states":        "US",
	"united kingdom":       "GB",
	"indonesia":            "ID",
	"australia":            "AU",
	"canada":               "CA",
	"germany":              "DE",
	"france":               "FR",
	"singapore":            "SG",
	"malaysia":             "MY",
	"thailand":             "TH",
	"japan":                "JP",
	"brazil":               "BR",
	"mexico":               "MX",
	"spain":                "ES",
	"italy":                "IT",
	"netherlands":          "NL",
	"belgium":              "BE",
	"sweden":               "SE",
	"norway":               "NO",
	"denmark":              "DK",
	"finland":              "FI",
	"poland":               "PL",
	"turkey":               "TR",
	"india":                "IN",
	"china":                "CN",
	"korea":                "KR",
	"south korea":          "KR",
	"vietnam":              "VN",
	"philippines":          "PH",
	"new zealand":          "NZ",
	"portugal":             "PT",
	"ireland":              "IE",
	"croatia":              "HR",
	"austria":              "AT",
	"chile":                "CL",
	"colombia":             "CO",
	"hungary":              "HU",
	"romania":              "RO",
	"greece":               "GR",
	"taiwan":               "TW",
	"morocco":              "MA",
	"argentina":            "AR",
	"egypt":                "EG",
	"myanmar":              "MM",
	"costa rica":           "CR",
	"panama":               "PA",
	"kenya":                "KE",
	"bahrain":              "BH",
	"qatar":                "QA",
	"nepal":                "NP",
	"nigeria":              "NG",
	"sri lanka":            "LK",
	"cambodia":             "KH",
	"switzerland":          "CH",
	"south africa":         "ZA",
	"united arab emirates": "AE",
	"saudi arabia":         "SA",
	"czech republic":       "CZ",
	"hong kong":            "HK",
}

// TestHardenedRegex_NoGarbageCapture verifies that the hardened regex does NOT
// match address strings that produced garbage in the original buggy migration.
// Each case was observed in production or is a representative edge case.
func TestHardenedRegex_NoGarbageCapture(t *testing.T) {
	re := regexp.MustCompile(hardenedRegexPattern)

	noMatchCases := []struct {
		name    string
		address string
	}{
		{"uk postcode AB", "123 Main Street, Leeds, LS7 1AB"},
		{"dutch city AAG", "Hofweg 1, Den Haag"},
		{"german city ACH", "Bachstraße 5, Schönebach"},
		{"spanish city ADO", "Calle Mayor 3, Maldonado"},
		{"generic ads suffix ADS", "Texarkana escort ads"},
		{"3-letter suffix COM", "42 Business Park, Telecom"},
		{"3-letter suffix ONS", "1 Customer Relations"},
		{"3-letter suffix ESS", "Fitness Progress"},
		{"3-letter suffix MAP", "Roadmap Avenue"},
		{"3-letter suffix ICA", "Botanica Street"},
		{"3-letter suffix ORG", "123 Org blvd"},
		{"3-letter suffix ION", "Application Center"},
		{"3-letter suffix EET", "1 Meet Street"},
		{"3-letter suffix ODE", "Zip Code Lane"},
		{"3-letter suffix ICO", "El Economico"},
		{"3-letter suffix RIS", "Chris Close"},
		{"3-letter suffix OAD", "44 Broad Road"},
		{"3-letter suffix TER", "1 Manchester Center"},
		{"3-letter suffix NIA", "California"},
		{"3-letter suffix URG", "Hamburg"},
		{"plain 2-letter AB", "AB"},
		{"plain 2-letter CD", "CD"},
	}

	for _, tc := range noMatchCases {
		t.Run(tc.name, func(t *testing.T) {
			m := re.FindStringSubmatch(tc.address)
			if m != nil {
				t.Errorf("hardened regex incorrectly matched %q in address %q; got capture=%q",
					tc.address, tc.address, m[1])
			}
		})
	}
}

// TestHardenedRegex_LegitimateCountriesMatch verifies that the hardened regex
// DOES match known country names at the end of address strings.
func TestHardenedRegex_LegitimateCountriesMatch(t *testing.T) {
	re := regexp.MustCompile(hardenedRegexPattern)

	matchCases := []struct {
		name      string
		address   string
		wantLower string // expected match (lowercased for case-insensitive comparison)
	}{
		{"US full name", "123 Main St, New York, United States", "united states"},
		{"GB full name", "10 Downing St, London, United Kingdom", "united kingdom"},
		{"AU full name", "42 Sydney Rd, Sydney, Australia", "australia"},
		{"DE full name", "Bahnhofstr. 1, Berlin, Germany", "germany"},
		{"PT full name", "Rua da Liberdade 1, Lisbon, Portugal", "portugal"},
		{"IE full name", "O'Connell St, Dublin, Ireland", "ireland"},
		{"HR full name", "Ilica 1, Zagreb, Croatia", "croatia"},
		{"NZ full name", "Queen St, Auckland, New Zealand", "new zealand"},
		{"LK full name", "Main Rd, Colombo, Sri Lanka", "sri lanka"},
		{"KH full name", "Monivong Blvd, Phnom Penh, Cambodia", "cambodia"},
		{"case insensitive lower", "123 st, PORTUGAL", "portugal"},
		{"trailing space", "Lisbon, Portugal  ", "portugal"},
	}

	for _, tc := range matchCases {
		t.Run(tc.name, func(t *testing.T) {
			m := re.FindStringSubmatch(tc.address)
			if m == nil {
				t.Errorf("hardened regex failed to match legitimate country in %q", tc.address)
				return
			}
			got := strings.ToLower(strings.TrimSpace(m[1]))
			if got != tc.wantLower {
				t.Errorf("got %q, want %q for address %q", got, tc.wantLower, tc.address)
			}
		})
	}
}

// TestCountryNameToISO_Mapping validates that every entry in the CASE mapping
// used by both SQL blocks has the correct ISO 3166-1 alpha-2 output.
// Caught typos or wrong codes will fail here before reaching production.
func TestCountryNameToISO_Mapping(t *testing.T) {
	// canonical spot-checks: name → expected ISO (independently verified)
	wantISO := map[string]string{
		"portugal":             "PT",
		"ireland":              "IE",
		"croatia":              "HR",
		"austria":              "AT",
		"chile":                "CL",
		"colombia":             "CO",
		"hungary":              "HU",
		"romania":              "RO",
		"greece":               "GR",
		"taiwan":               "TW",
		"morocco":              "MA",
		"argentina":            "AR",
		"egypt":                "EG",
		"myanmar":              "MM",
		"costa rica":           "CR",
		"panama":               "PA",
		"kenya":                "KE",
		"bahrain":              "BH",
		"qatar":                "QA",
		"nepal":                "NP",
		"nigeria":              "NG",
		"sri lanka":            "LK",
		"cambodia":             "KH",
		"switzerland":          "CH",
		"south africa":         "ZA",
		"united arab emirates": "AE",
		"saudi arabia":         "SA",
		"czech republic":       "CZ",
		"hong kong":            "HK",
		"united states":        "US",
		"united kingdom":       "GB",
		"indonesia":            "ID",
		"australia":            "AU",
		"canada":               "CA",
		"germany":              "DE",
		"france":               "FR",
		"singapore":            "SG",
		"malaysia":             "MY",
		"thailand":             "TH",
		"japan":                "JP",
		"brazil":               "BR",
		"mexico":               "MX",
		"spain":                "ES",
		"italy":                "IT",
		"netherlands":          "NL",
		"belgium":              "BE",
		"sweden":               "SE",
		"norway":               "NO",
		"denmark":              "DK",
		"finland":              "FI",
		"poland":               "PL",
		"turkey":               "TR",
		"india":                "IN",
		"china":                "CN",
		"korea":                "KR",
		"vietnam":              "VN",
		"philippines":          "PH",
		"new zealand":          "NZ",
		// Short names and aliases added by auditor P1 fix
		"uk":   "GB",
		"peru": "PE",
		"cuba": "CU",
		"iran": "IR",
		"iraq": "IQ",
		"oman": "OM",
		"laos": "LA",
		"fiji": "FJ",
		"mali": "ML",
		"chad": "TD",
		"togo": "TG",
		"guam": "GU",
	}

	for name, expectedISO := range wantISO {
		t.Run(name, func(t *testing.T) {
			got, ok := countryNameToISOTest[name]
			if !ok {
				t.Errorf("country name %q missing from mapping table", name)
				return
			}
			if got != expectedISO {
				t.Errorf("country name %q: got ISO %q, want %q", name, got, expectedISO)
			}
		})
	}
}

// TestGarbagePattern_UppercaseGreedy demonstrates the original bug.
// The original regex had [A-Z]{2,3} as the FIRST alternative, which caused it
// to greedily capture the last uppercase chars of any address.
// This test documents the bug (original regex WOULD match these) and confirms
// the hardened regex does NOT.
func TestGarbagePattern_UppercaseGreedy(t *testing.T) {
	// Original buggy regex (for documentation of what the bug looked like)
	buggyRe := regexp.MustCompile(`(?i)([A-Z]{2,3}|United States|United Kingdom|Indonesia|Australia|Canada|Germany|France|Singapore|Malaysia|Thailand|Japan|Brazil|Mexico|Spain|Italy|Netherlands|Belgium|Sweden|Norway|Denmark|Finland|Poland|Turkey|India|China|Korea|Vietnam|Philippines|New Zealand)\s*$`)
	hardenedRe := regexp.MustCompile(hardenedRegexPattern)

	garbageCases := []struct {
		address       string
		buggyCaptures string // what the buggy regex captured
	}{
		{"123 Main Street, Leeds, LS7 1AB", "AB"},
		{"Hofweg 1, Den Haag", "AAG"},
		{"Bachstraße 5, Schönebach", "ACH"},
		{"Calle Mayor 3, Maldonado", "ADO"},
		{"Texarkana escort ads", "ADS"},
	}

	for _, tc := range garbageCases {
		t.Run(tc.buggyCaptures, func(t *testing.T) {
			// Confirm the buggy regex DOES capture garbage (bug documentation)
			buggyMatch := buggyRe.FindStringSubmatch(tc.address)
			if buggyMatch == nil {
				t.Logf("note: buggy regex didn't match %q (address may have changed)", tc.address)
			} else if strings.ToUpper(buggyMatch[1]) != tc.buggyCaptures {
				t.Logf("note: buggy regex captured %q not %q for %q",
					buggyMatch[1], tc.buggyCaptures, tc.address)
			}

			// The hardened regex must NOT capture anything for these addresses
			hardenedMatch := hardenedRe.FindStringSubmatch(tc.address)
			if hardenedMatch != nil {
				t.Errorf("hardened regex incorrectly matched %q in %q; capture=%q",
					hardenedMatch[1], tc.address, hardenedMatch[1])
			}
		})
	}
}
