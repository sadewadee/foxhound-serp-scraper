package directory

import (
	"strings"
	"testing"
)

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
