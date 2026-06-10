package db

import (
	"strings"
	"testing"
)

// buildCityAlternation feeds one regexp_match per queries row in the geo
// backfill — ordering and escaping bugs would silently mis-attribute geo.
func TestBuildCityAlternation(t *testing.T) {
	rows := []GeoCity{
		{CityLower: "york", City: "York", CountryCode: "GB"},
		{CityLower: "new york", City: "New York", CountryCode: "US"},
		{CityLower: "st. petersburg", City: "St. Petersburg", CountryCode: "US"},
		{CityLower: "", City: "", CountryCode: "XX"}, // must be dropped
	}
	alt := buildCityAlternation(rows)

	if !strings.HasPrefix(alt, `\m(`) || !strings.HasSuffix(alt, `)\M`) {
		t.Fatalf("alternation must be word-bounded, got %q", alt)
	}
	// Longest-first: "st\. petersburg" (escaped) > "new york" > "york".
	iNY := strings.Index(alt, `new york`)
	iY := strings.LastIndex(alt, `york`)
	if iNY == -1 || iY == -1 || iNY > iY {
		t.Errorf("longer token must precede shorter: %q", alt)
	}
	// Regex metacharacters must be escaped.
	if !strings.Contains(alt, `st\. petersburg`) {
		t.Errorf("dots must be escaped: %q", alt)
	}
	// Empty city tokens must not produce an empty alternative (would match everything).
	if strings.Contains(alt, "||") || strings.Contains(alt, "(|") || strings.Contains(alt, "|)") {
		t.Errorf("empty alternative leaked into alternation: %q", alt)
	}
}
