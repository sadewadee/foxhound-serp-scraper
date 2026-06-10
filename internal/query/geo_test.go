//go:build playwright

package query

import (
	"strings"
	"testing"

	"github.com/sadewadee/serp-scraper/internal/db"
)

// Every Cities country MUST have an ISO-2 code — a missing entry silently
// drops that country's cities from geo_cities and its queries/listings never
// get geo attribution (the exact failure mode that left 86% of listings
// country-less).
func TestCountryISOCoversAllCities(t *testing.T) {
	for country := range Cities {
		code, ok := CountryISO[country]
		if !ok {
			t.Errorf("Cities country %q has no CountryISO entry", country)
			continue
		}
		if len(code) != 2 || code != strings.ToUpper(code) {
			t.Errorf("CountryISO[%q] = %q — must be ISO-3166-1 alpha-2 uppercase", country, code)
		}
	}
}

func TestGeoCityRows(t *testing.T) {
	rows := GeoCityRows()
	if len(rows) == 0 {
		t.Fatal("GeoCityRows returned nothing")
	}

	vocab := nicheVocabulary()
	byLower := make(map[string]db.GeoCity, len(rows))
	for _, r := range rows {
		if r.CityLower != strings.ToLower(r.CityLower) {
			t.Errorf("CityLower %q is not lowercase", r.CityLower)
		}
		if vocab[r.CityLower] {
			t.Errorf("city token %q collides with niche/template vocabulary — would mis-attribute geo on every query containing it", r.CityLower)
		}
		if _, dup := byLower[r.CityLower]; dup {
			t.Errorf("duplicate city token %q", r.CityLower)
		}
		byLower[r.CityLower] = r
	}

	// Anchor expectations for the SE-Asia gap this work fixes.
	if got := byLower["jakarta"]; got.CountryCode != "ID" || got.City != "Jakarta" {
		t.Errorf("jakarta = %+v, want City=Jakarta CountryCode=ID", got)
	}
	if got := byLower["bali"]; got.CountryCode != "ID" {
		t.Errorf("bali country = %q, want ID", got.CountryCode)
	}
	// Multi-word token survives intact (matched longest-first downstream).
	if got := byLower["kuta lombok"]; got.CountryCode != "ID" {
		t.Errorf("kuta lombok = %+v, want CountryCode=ID", got)
	}
}

// isoAlpha2 is the canonical ISO 3166-1 alpha-2 set, used to guard every code we
// seed into the countries lookup / alias map against invariant #6 (codes only
// from a curated whitelist — a typo'd "UAE"-style 3-letter or invented code must
// fail the build, not pollute country_code).
var isoAlpha2 = func() map[string]bool {
	const all = "AD AE AF AG AI AL AM AO AQ AR AS AT AU AW AX AZ BA BB BD BE BF BG BH BI BJ BL BM BN BO BQ BR BS BT BV BW BY BZ CA CC CD CF CG CH CI CK CL CM CN CO CR CU CV CW CX CY CZ DE DJ DK DM DO DZ EC EE EG EH ER ES ET FI FJ FK FM FO FR GA GB GD GE GF GG GH GI GL GM GN GP GQ GR GS GT GU GW GY HK HM HN HR HT HU ID IE IL IM IN IO IQ IR IS IT JE JM JO JP KE KG KH KI KM KN KP KR KW KY KZ LA LB LC LI LK LR LS LT LU LV LY MA MC MD ME MF MG MH MK ML MM MN MO MP MQ MR MS MT MU MV MW MX MY MZ NA NC NE NF NG NI NL NO NP NR NU NZ OM PA PE PF PG PH PK PL PM PN PR PS PT PW PY QA RE RO RS RU RW SA SB SC SD SE SG SH SI SJ SK SL SM SN SO SR SS ST SV SX SY SZ TC TD TF TG TH TJ TK TL TM TN TO TR TT TV TW TZ UA UG UM US UY UZ VA VC VE VG VI VN VU WF WS YE YT ZA ZM ZW"
	m := map[string]bool{}
	for _, c := range strings.Fields(all) {
		m[c] = true
	}
	return m
}()

func TestCountryRows(t *testing.T) {
	rows := CountryRows()
	want := len(CountryISO) + len(extraLookupCountries)
	if len(rows) != want {
		t.Fatalf("CountryRows len = %d, want %d (CountryISO %d + extras %d)", len(rows), want, len(CountryISO), len(extraLookupCountries))
	}
	seen := map[string]string{}
	for _, r := range rows {
		if _, dup := seen[r.Code]; dup {
			t.Errorf("duplicate country code %q", r.Code)
		}
		if len(r.Code) != 2 || r.Code != strings.ToUpper(r.Code) || !isoAlpha2[r.Code] {
			t.Errorf("country code %q is not a valid ISO-3166-1 alpha-2 code", r.Code)
		}
		seen[r.Code] = r.Name
	}
	// Canonical lookup names must match page-extraction forms (the AE/"UAE" and
	// missing-country gaps this work closes).
	if seen["AE"] != "United Arab Emirates" {
		t.Errorf(`AE name = %q, want "United Arab Emirates"`, seen["AE"])
	}
	if seen["CN"] != "China" {
		t.Errorf(`CN name = %q, want "China"`, seen["CN"])
	}
	for _, c := range []string{"ID", "US", "TH", "AE", "CN", "RU", "PK"} {
		if _, ok := seen[c]; !ok {
			t.Errorf("expected %s in CountryRows", c)
		}
	}
}

func TestCountryAliasesValid(t *testing.T) {
	codes := map[string]bool{}
	for _, r := range CountryRows() {
		codes[r.Code] = true
	}
	for alias, code := range CountryAliases() {
		if alias != strings.ToLower(alias) {
			t.Errorf("alias %q must be lowercased (matched against LOWER(bl.country))", alias)
		}
		if !isoAlpha2[code] {
			t.Errorf("alias %q → %q is not a valid ISO-3166-1 alpha-2 code", alias, code)
		}
		if !codes[code] {
			t.Errorf("alias %q → %q has no countries row (FK gap — backfill would map to a code absent from the lookup)", alias, code)
		}
	}
}
