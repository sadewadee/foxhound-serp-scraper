//go:build playwright

package scraper

import (
	"os"
	"testing"
)

func TestTailParseCountry(t *testing.T) {
	cases := []struct {
		name string
		addr string
		want string
	}{
		// Verified production rows from the audit.
		{"marineandboat US", "521 Oak Grove Rd, Flat Rock, NC, United States", "US"},
		{"officialalphaland US", "Missouri City, TX 77489, United States", "US"},

		// Country at tail in various forms.
		{"GB at tail", "10 Downing St, London, United Kingdom", "GB"},
		{"GB England", "1 High St, London, England", "GB"},
		{"ID full name", "Jl. Sudirman 1, Jakarta, Indonesia", "ID"},
		{"NL full name", "Compagnonsplein 1, 1234 AB Amsterdam, Netherlands", "NL"},
		{"AU at tail", "1 George St, Sydney, NSW 2000, Australia", "AU"},
		{"alpha-3 USA", "1 Main St, USA", "US"},
		{"alpha-3 GBR", "1 Main St, GBR", "GB"},
		{"trailing punct", "1 Main St, Indonesia.", "ID"},
		{"trailing space", "1 Main St, Indonesia ", "ID"},

		// Must NOT match — state codes & junk.
		{"US state CA alone", "1 Infinite Loop, Cupertino, CA 95014", ""},
		{"US state NC alone", "Foo, Flat Rock, NC", ""},
		{"no comma cvsf.nl style", "Compagnonsplein 1", ""},
		{"empty", "", ""},
		{"single token", "FooBar", ""},
		{"only zip", "12345", ""},
		{"state with zip combined token", "TX 77489", ""},

		// Tail-walk: country deeper than last token.
		{"country before postal token", "1 Main St, Jakarta, Indonesia, 12345", "ID"},

		// REAL PRODUCTION rows (DB verified, 2026-05-11) — verifies tail-walk
		// across the actual address shapes seen in business_listings.
		{"canada postal at tail", "1137, Derry Road East, Mississauga, ON, Canada, L5T 1P3", "CA"},
		{"sg with newline", "20 Leonie Hill, #06-22, Singapore, 239222\nSingapore", "SG"},
		{"montreal canada postal", "1250 Boulevard René Lévesque Ouest, Montréal, Canada, H3B 4W8", "CA"},
		{"uk postal tail", "32 Lake Rd, Bowness-on-Windermere, Windermere LA23 3AP, United Kingdom", "GB"},
		{"frankfurt germany", "Frankfurt, Germany", "DE"},
		{"paris france", "75 bis Avenue Marceau, 75116 Paris, France", "FR"},
		{"france postal", "ZI du bois de Leuze 12 rue Denis Papin, St Martin de Crau, France, 13310", "FR"},
		{"jakarta indo", "AD Premier 9th floor, Jl. TB Simatupang No.5 Ragunan, Pasar Minggu, Jakarta Selatan 12550, DKI Jakarta, Indonesia", "ID"},
		{"germany de-ni", "Hildesheim, DE-NI, Germany", "DE"},
		{"india tail", "303, Camps Corner-II, Nr. Prahladnagar Garden, Satellite, Ahmedabad – 380 015. Gujarat, India", "IN"},
		{"comma sep zip then country", "8344 Foothill Blvd, Sunland, CA, 91040, United States", "US"},

		// Right-anchored word-slice fallback (DB-verified embedded-newline shapes).
		{"singapore postal newline", "20 Leonie Hill, #06-22, Singapore, 239222\nSingapore", "SG"},
		{"nz postal newline", "5/23 Waring Taylor Street\nWellington, Wellington, 6011\nNew Zealand", "NZ"},
		{"ph postal trailing word", "522 J.A. Clarin St, Tagbilaran City, 6300 Bohol, Philippines", "PH"},
		{"mexico with period", "TV AZTECA Periférico Sur 4121, Ciudad De México, México.", "MX"},

		// Word-slice must NOT create false positives mid-segment.
		{"usa drive false positive guard", "1 USA Drive, Some City", ""},
		{"africa street guard", "South Africa Street, Boston, MA, 02134", ""},

		// Sprint 6: collision heuristic for ID/IL/IN (Indonesia/Israel/India vs
		// Idaho/Illinois/Indiana). ACCEPT when non-US shape; REJECT when US.
		{"indonesia ID at tail with 5-digit postal", "Jl. Sudirman 1, Jakarta Selatan, 12550, ID", "ID"},
		{"indonesia ID compact short form", "Some St, 80572, ID", "ID"},
		{"israel IL with 7-digit postal", "21 Abba Hillel Silver Rd, Ramat Gan, 5252213, IL", "IL"},
		{"india IN with 6-digit PIN", "Manjalikulam Rd, Thiruvananthapuram, 695001, IN", "IN"},
		{"india IN mumbai PIN", "Some St, Mumbai, 400001, IN", "IN"},
		{"reject IL when illinois city explicit", "1 Main St, Chicago, IL, 60601", ""},
		{"reject IN when NY state code preceding", "1 Main St, NY 12345, IN", ""},
		{"reject ID with idaho full name", "Some St, Idaho, 83001, ID", ""},
		{"reject IL with illinois full name", "Some St, Illinois, 60601, IL", ""},
		{"reject IN with indiana full name", "Some St, Indiana, 46001, IN", ""},
		{"reject ID no preceding postal", "Some Address, ID", ""},
		{"reject ID non-numeric preceding", "Some St, City Name, ID", ""},
		{"reject ID too-few-digit preceding", "Some St, 123, ID", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tailParseCountry(tc.addr)
			if got != tc.want {
				t.Errorf("tailParseCountry(%q) = %q; want %q", tc.addr, got, tc.want)
			}
		})
	}
}

func TestTailParseCity_USStyle(t *testing.T) {
	cases := []struct {
		name string
		addr string
		want string
	}{
		// US-style address: ..., <city>, <STATE_CODE> [zip][, country]
		{"marineandboat city", "521 Oak Grove Rd, Flat Rock, NC, United States", "Flat Rock"},
		{"officialalphaland city", "Missouri City, TX 77489, United States", "Missouri City"},
		{"cupertino", "1 Infinite Loop, Cupertino, CA 95014", "Cupertino"},
		{"san antonio no zip", "1 Main St, San Antonio, TX", "San Antonio"},
		{"city is first token", "Missouri City, TX 77489", "Missouri City"},
		{"city with extended zip", "1 Main, Foo Town, CA 90210-1234", "Foo Town"},

		// REAL PRODUCTION rows (DB verified, 2026-05-11) — comma-separated
		// zip form: <city>, <STATE>, <ZIP>, <country>. This form was the
		// original regex's blind spot.
		{"sunland prod", "8344 Foothill Blvd, Sunland, CA, 91040, United States", "Sunland"},
		{"swannanoa prod", "701 Warren Wilson Rd, Swannanoa, NC, 28778, USA", "Swannanoa"},
		{"englewood prod", "61 West Palisade 2B, Englewood, NJ, 07631, USA", "Englewood"},
		{"schaumburg prod", "1375   E Schaumburg Rd #100, Schaumburg, IL, 60194, USA", "Schaumburg"},
		{"new york prod", "40 West 25th Street, 4th Fl, New York, NY, 10010, United States", "New York"},
		{"dallas prod", "4100 Alpha Rd, Dallas, TX 75244, USA", "Dallas"},
		{"streamwood prod", "1092 Frances Dr, Streamwood, IL 60107, USA", "Streamwood"},

		// Must NOT match — non-US format or insufficient signal.
		{"non-US Dutch", "Compagnonsplein 1, 1234 AB Amsterdam, Netherlands", ""},
		{"no state pattern", "Just City, Indonesia", ""},
		{"single token", "FooBar", ""},
		{"empty", "", ""},
		{"state-like but not US state", "Foo, GB, Bar", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tailParseCity(tc.addr)
			if got != tc.want {
				t.Errorf("tailParseCity(%q) = %q; want %q", tc.addr, got, tc.want)
			}
		})
	}
}

func TestParseAddressFallback(t *testing.T) {
	cases := []struct {
		name        string
		addr        string
		wantCountry string
		wantCity    string
	}{
		{"empty", "", "", ""},
		{"both filled US-style", "521 Oak Grove Rd, Flat Rock, NC, United States", "US", "Flat Rock"},
		{"country only no US city", "Compagnonsplein 1, 1234 AB Amsterdam, Netherlands", "NL", ""},
		{"country only newline embedded", "20 Leonie Hill, Singapore, 239222\nSingapore", "SG", ""},
		{"neither resolves", "Compagnonsplein 1", "", ""},
		{"both empty inputs", "   ", "", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gotCountry, gotCity := ParseAddressFallback(tc.addr)
			if gotCountry != tc.wantCountry {
				t.Errorf("ParseAddressFallback(%q) country = %q; want %q",
					tc.addr, gotCountry, tc.wantCountry)
			}
			if gotCity != tc.wantCity {
				t.Errorf("ParseAddressFallback(%q) city = %q; want %q",
					tc.addr, gotCity, tc.wantCity)
			}
		})
	}
}

func TestTLDCountryHint(t *testing.T) {
	cases := []struct {
		name string
		host string
		want string
	}{
		// Allowlisted ccTLDs.
		{"NL", "cvsf.nl", "NL"},
		{"NL with www", "www.cvsf.nl", "NL"},
		{"DE", "example.de", "DE"},
		{"FR", "example.fr", "FR"},
		{"AU", "example.au", "AU"},
		{"ID", "example.id", "ID"},
		{"CH", "example.ch", "CH"},
		{"JP", "example.jp", "JP"},

		// Compound ccTLDs.
		{"co.uk", "example.co.uk", "GB"},
		{"com.au", "example.com.au", "AU"},
		{"co.id", "example.co.id", "ID"},
		{"co.jp", "example.co.jp", "JP"},
		{"com.br", "example.com.br", "BR"},

		// Subdomain handling.
		{"deep subdomain", "shop.eu.example.de", "DE"},

		// Excluded generic / ambiguous TLDs.
		{"com", "example.com", ""},
		{"net", "example.net", ""},
		{"org", "example.org", ""},
		{"io is ccTLD but generic in practice", "example.io", ""},
		{"co generic", "example.co", ""},
		{"me generic", "example.me", ""},
		{"tv generic", "example.tv", ""},
		{"app generic", "example.app", ""},
		{"empty", "", ""},
		{"no dot", "localhost", ""},

		// With URL scheme stripping (function should handle host-only).
		{"trailing dot", "example.de.", "DE"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := tldCountryHint(tc.host)
			if got != tc.want {
				t.Errorf("tldCountryHint(%q) = %q; want %q", tc.host, got, tc.want)
			}
		})
	}
}

func TestApplyTLDCountryFallback_EnvGated(t *testing.T) {
	t.Run("disabled by default", func(t *testing.T) {
		os.Unsetenv("COUNTRY_TLD_FALLBACK_ENABLED")
		cd := &ContactData{}
		ApplyTLDCountryFallback(cd, "https://cvsf.nl/about")
		if cd.Country != "" {
			t.Errorf("flag unset: want Country='', got %q", cd.Country)
		}
	})
	t.Run("explicit false is no-op", func(t *testing.T) {
		t.Setenv("COUNTRY_TLD_FALLBACK_ENABLED", "false")
		cd := &ContactData{}
		ApplyTLDCountryFallback(cd, "https://cvsf.nl/about")
		if cd.Country != "" {
			t.Errorf("flag=false: want Country='', got %q", cd.Country)
		}
	})
	t.Run("flag true fills empty Country", func(t *testing.T) {
		t.Setenv("COUNTRY_TLD_FALLBACK_ENABLED", "true")
		cd := &ContactData{}
		ApplyTLDCountryFallback(cd, "https://cvsf.nl/about")
		if cd.Country != "NL" {
			t.Errorf("flag=true cvsf.nl: want Country='NL', got %q", cd.Country)
		}
	})
	t.Run("flag true does not overwrite existing Country", func(t *testing.T) {
		t.Setenv("COUNTRY_TLD_FALLBACK_ENABLED", "true")
		cd := &ContactData{Country: "US"}
		ApplyTLDCountryFallback(cd, "https://example.nl/")
		if cd.Country != "US" {
			t.Errorf("existing Country must not be overwritten: want 'US', got %q", cd.Country)
		}
	})
	t.Run("flag true, empty URL is no-op", func(t *testing.T) {
		t.Setenv("COUNTRY_TLD_FALLBACK_ENABLED", "true")
		cd := &ContactData{}
		ApplyTLDCountryFallback(cd, "")
		if cd.Country != "" {
			t.Errorf("empty URL: want Country='', got %q", cd.Country)
		}
	})
	t.Run("flag true, generic TLD is no-op", func(t *testing.T) {
		t.Setenv("COUNTRY_TLD_FALLBACK_ENABLED", "true")
		cd := &ContactData{}
		ApplyTLDCountryFallback(cd, "https://example.com/")
		if cd.Country != "" {
			t.Errorf("generic TLD: want Country='', got %q", cd.Country)
		}
	})
	t.Run("flag true, malformed URL is no-op", func(t *testing.T) {
		t.Setenv("COUNTRY_TLD_FALLBACK_ENABLED", "true")
		cd := &ContactData{}
		ApplyTLDCountryFallback(cd, "::not-a-url")
		if cd.Country != "" {
			t.Errorf("malformed URL: want Country='', got %q", cd.Country)
		}
	})
}
