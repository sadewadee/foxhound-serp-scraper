package directory

import (
	"strings"
	"testing"
)

func TestExtractFromJSONLD_TypeArray(t *testing.T) {
	// 2026-09-25: @type can be an array (a node belonging to multiple
	// schema.org types at once, e.g. ["LocalBusiness","HealthClub"]) — the
	// previous `ld["@type"].(string)` assertion silently returned "" for it.
	ld := map[string]any{
		"@type": []any{"LocalBusiness", "HealthClub"},
		"name":  "Test Gym",
	}
	l := extractFromJSONLD(ld, "test")
	if l.Category != "LocalBusiness" {
		t.Errorf("Category = %q; want %q (first entry of the @type array)", l.Category, "LocalBusiness")
	}
}

func TestExtractFromJSONLD_TypeString(t *testing.T) {
	ld := map[string]any{"@type": "Restaurant", "name": "Test Cafe"}
	l := extractFromJSONLD(ld, "test")
	if l.Category != "Restaurant" {
		t.Errorf("Category = %q; want %q", l.Category, "Restaurant")
	}
}

func TestExtractFromJSONLD_IncludesAddressCountry(t *testing.T) {
	ld := map[string]any{
		"address": map[string]any{
			"streetAddress":   "521 Oak Grove Rd",
			"addressLocality": "Flat Rock",
			"addressRegion":   "NC",
			"postalCode":      "28731",
			"addressCountry":  "United States",
		},
	}
	l := extractFromJSONLD(ld, "test")
	if !strings.Contains(l.Address, "United States") {
		t.Errorf("Address should include addressCountry; got %q", l.Address)
	}
}
