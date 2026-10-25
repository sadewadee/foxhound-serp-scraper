package prodir

import "testing"

// Names directories emit that used to resolve to nothing (2026-10 prodir loads),
// plus UN long forms. Ambiguous or non-ISO names must stay unresolved: a code is
// only ever taken from the curated lookup (invariant #6).
func TestCountryResolutionGaps(t *testing.T) {
	r := newCountryResolver()
	want := map[string]string{
		"Albania": "AL", "Algeria": "DZ", "Andorra": "AD", "Iran": "IR", "Kazakhstan": "KZ",
		"Bolivia": "BO", "Ghana": "GH", "Jersey": "JE", "Uzbekistan": "UZ", "Venezuela": "VE",
		"Reunion": "RE", "Réunion": "RE",
		"Scotland": "GB", "England": "GB", "Wales": "GB", "Northern Ireland": "GB",
		"Korea (the Republic of)": "KR", "Korea, Republic of": "KR", "Republic of Korea": "KR",
		"Iran (Islamic Republic of)": "IR", "Bolivia (Plurinational State of)": "BO",
		"Venezuela (Bolivarian Republic of)": "VE", "Moldova (the Republic of)": "MD",
		"Tanzania, United Republic of": "TZ", "Russian Federation": "RU", "Viet Nam": "VN",
		"Czechia": "CZ", "Czech Republic": "CZ", "Turkey": "TR", "Turkiye": "TR", "Türkiye": "TR",
		"Guinea": "GN", "Niger": "NE", "Sudan": "SD", "South Sudan": "SS", "Nigeria": "NG",
		"Guinea-Bissau": "GW", "Equatorial Guinea": "GQ", "Papua New Guinea": "PG",
		"Democratic Republic of the Congo": "CD", "Republic of the Congo": "CG",
		"Côte d'Ivoire": "CI", "Ivory Coast": "CI", "DE": "DE", "dz": "DZ",
	}
	for raw, code := range want {
		if _, got := r.resolve(raw); got != code {
			t.Errorf("resolve(%q) code = %q, want %q", raw, got, code)
		}
	}
	for _, raw := range []string{
		"Georgia",     // US state vs country: ambiguous, skipped
		"Congo",       // CD vs CG
		"Korea",       // KR vs KP
		"West Indies", // region, not an ISO country
		"Macedonia",   // ambiguous with the Greek region
		"Atlantis", "ZZ", "XK", "AB", "COM", "Europe", "Africa",
	} {
		if name, code := r.resolve(raw); code != "" || name != "" {
			t.Errorf("resolve(%q) = (%q,%q), want unresolved", raw, name, code)
		}
	}
}
