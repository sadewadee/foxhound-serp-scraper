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

func TestCountryRows(t *testing.T) {
	rows := CountryRows()
	if len(rows) != len(CountryISO) {
		t.Fatalf("CountryRows len = %d, want %d", len(rows), len(CountryISO))
	}
	seen := map[string]bool{}
	for _, r := range rows {
		if seen[r.Code] {
			t.Errorf("duplicate country code %q", r.Code)
		}
		seen[r.Code] = true
	}
	if !seen["ID"] || !seen["US"] || !seen["TH"] {
		t.Error("expected ID/US/TH in CountryRows")
	}
}
