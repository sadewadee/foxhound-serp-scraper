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

		// US state + ZIP → US (issue #26 policy: state+ZIP is an unambiguous
		// US signal; previously these were left empty, the dominant empty class).
		{"US state CA + zip", "1 Infinite Loop, Cupertino, CA 95014", "US"},
		{"state TX + zip combined", "TX 77489", "US"},
		{"US full state name + zip", "5251 Westheimer Rd, Houston, Texas, 77056", "US"},
		{"US PA + zip", "3535 Pine Avenue, Erie, PA 16504", "US"},
		{"US IA + zip", "105 E. 9th St., Coralville, IA 52241", "US"},
		{"US CA comma zip", "17592 Irvine Blvd, Tustin, CA, 92780", "US"},

		// Must NOT match — state code without ZIP, junk, non-US postal-first.
		{"US state NC alone", "Foo, Flat Rock, NC", ""},
		{"no comma cvsf.nl style", "Compagnonsplein 1", ""},
		{"empty", "", ""},
		{"single token", "FooBar", ""},
		{"only zip", "12345", ""},
		{"german postal-first not US", "Altmarkt, 01067 Dresden", ""},
		{"french postal not US", "7 Rue Linois, Paris, 75015", ""},

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
		// "South Africa Street" must not match ZA — but MA + 02134 IS US, so
		// the correct answer is US (street-name guard still holds: not ZA).
		{"africa street guard not ZA but US via MA zip", "South Africa Street, Boston, MA, 02134", "US"},

		// Sprint 6: collision heuristic for ID/IL/IN (Indonesia/Israel/India vs
		// Idaho/Illinois/Indiana). ACCEPT when non-US shape; REJECT when US.
		{"indonesia ID at tail with 5-digit postal", "Jl. Sudirman 1, Jakarta Selatan, 12550, ID", "ID"},
		{"indonesia ID compact short form", "Some St, 80572, ID", "ID"},
		{"israel IL with 7-digit postal", "21 Abba Hillel Silver Rd, Ramat Gan, 5252213, IL", "IL"},
		{"india IN with 6-digit PIN", "Manjalikulam Rd, Thiruvananthapuram, 695001, IN", "IN"},
		{"india IN mumbai PIN", "Some St, Mumbai, 400001, IN", "IN"},
		// IL/IN here are US states with ZIPs → US (not Israel/India). The
		// collision rescue correctly declines; the state+ZIP fallback tags US.
		{"chicago IL + zip is US not israel", "1 Main St, Chicago, IL, 60601", "US"},
		{"NY zip is US not india", "1 Main St, NY 12345, IN", "US"},
		// Idaho/Illinois/Indiana + ZIP → US (the state-name+ZIP is a real US
		// address; "ID"/"IL"/"IN" here are the state abbrevs, not country codes).
		{"idaho full name + zip is US", "Some St, Idaho, 83001, ID", "US"},
		{"illinois full name + zip is US", "Some St, Illinois, 60601, IL", "US"},
		{"indiana full name + zip is US", "Some St, Indiana, 46001, IN", "US"},
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

// TestExtractContacts_JSONLD_GraphFlatten covers root cause #2 of the
// 2026-09-25 schema.org-overrides-keyword incident: a Yoast/RankMath-style
// "@graph" wrapper (WebSite + WebPage + Organization + Person + the real
// business node) must be flattened so the specific business @type
// (HealthClub) wins BusinessCategory instead of being invisible entirely.
func TestExtractContacts_JSONLD_GraphFlatten(t *testing.T) {
	html := `<html><head>
<script type="application/ld+json">
{
  "@context": "https://schema.org",
  "@graph": [
    {"@type": "WebSite", "name": "Example Site", "url": "https://example.com"},
    {"@type": "WebPage", "name": "Contact Us"},
    {"@type": "Organization", "name": "Example Org"},
    {"@type": "Person", "name": "Jane Doe"},
    {"@type": "HealthClub", "name": "Sunrise Health Club", "description": "Friendly neighborhood gym"}
  ]
}
</script>
</head><body></body></html>`

	cd := ExtractContacts([]byte(html))
	if cd.BusinessCategory != "HealthClub" {
		t.Errorf("BusinessCategory = %q; want %q (the real business node inside @graph, not the WebSite/Organization/Person wrapper)", cd.BusinessCategory, "HealthClub")
	}
	if cd.BusinessName != "Sunrise Health Club" {
		t.Errorf("BusinessName = %q; want %q (should prefer the node the chosen category came from)", cd.BusinessName, "Sunrise Health Club")
	}
}

// TestExtractContacts_JSONLD_TypeArray covers root cause #2's other half:
// "@type" as an array (a node belonging to multiple schema.org types at
// once) must not be silently dropped by a `.(string)` type assertion — and
// the MORE SPECIFIC member of the array (DaySpa) must win over the generic
// one (LocalBusiness).
func TestExtractContacts_JSONLD_TypeArray(t *testing.T) {
	html := `<html><head>
<script type="application/ld+json">
{"@type": ["LocalBusiness", "DaySpa"], "name": "Zen Day Spa"}
</script>
</head><body></body></html>`

	cd := ExtractContacts([]byte(html))
	if cd.BusinessCategory != "DaySpa" {
		t.Errorf("BusinessCategory = %q; want %q (specific type in the @type array should win over the generic LocalBusiness)", cd.BusinessCategory, "DaySpa")
	}
	if cd.BusinessName != "Zen Day Spa" {
		t.Errorf("BusinessName = %q; want %q", cd.BusinessName, "Zen Day Spa")
	}
}

// TestExtractContacts_JSONLD_ContentOnlyStaysEmpty covers root cause #1: a
// page whose ONLY JSON-LD nodes are content/page-structure types (Article,
// Person) must leave BusinessCategory empty rather than picking one of them
// — "Article" and "Person" are never a business category.
func TestExtractContacts_JSONLD_ContentOnlyStaysEmpty(t *testing.T) {
	html := `<html><head>
<script type="application/ld+json">
{"@graph": [
  {"@type": "Article", "headline": "5 Tips for Better Sleep"},
  {"@type": "Person", "name": "John Smith"}
]}
</script>
</head><body></body></html>`

	cd := ExtractContacts([]byte(html))
	if cd.BusinessCategory != "" {
		t.Errorf("BusinessCategory = %q; want empty (Article/Person are content types, never a business category)", cd.BusinessCategory)
	}
	// NOTE: BusinessName is intentionally NOT asserted empty here. Neither
	// node wins the category pick (both are tier-3/never), so the field
	// scan falls back to unchanged pre-existing behavior: the first node
	// with a "name" field wins, regardless of its @type. Person legitimately
	// has "name": "John Smith", so it fills BusinessName — that is a
	// pre-existing, separate concern from BusinessCategory (this fix's
	// scope) and is not something this change regresses or improves.
}

// TestExtractContacts_MetaKeywordsNoLongerFillCategory covers the removed
// fallback: <meta name="keywords"> is SEO keyword soup, not a category, and
// must no longer land in BusinessCategory (it used to, and the trigger's
// LENGTH(raw_category) > 100 rule then wrongly excluded real businesses).
func TestExtractContacts_MetaKeywordsNoLongerFillCategory(t *testing.T) {
	html := `<html><head>
<meta name="keywords" content="yoga, pilates, wellness, spa, meditation, jakarta, bali, retreat">
</head><body></body></html>`

	cd := ExtractContacts([]byte(html))
	if cd.BusinessCategory != "" {
		t.Errorf("BusinessCategory = %q; want empty (meta keywords must not fill BusinessCategory)", cd.BusinessCategory)
	}
}

// TestPickJSONLDCategory_SpecificityOverFirstWins is a focused unit test on
// the picker itself: a generic wrapper appearing FIRST in document order
// must lose to a more specific type appearing later — "first node wins" was
// the bug (root cause #1); "most specific wins" is the fix.
func TestPickJSONLDCategory_SpecificityOverFirstWins(t *testing.T) {
	nodes := []map[string]any{
		{"@type": "Organization", "name": "Wrapper Org"},
		{"@type": "YogaStudio", "name": "Real Yoga Studio"},
	}
	pick := pickJSONLDCategory(nodes)
	if pick == nil {
		t.Fatal("pickJSONLDCategory returned nil; want a pick")
	}
	if pick.category != "YogaStudio" {
		t.Errorf("category = %q; want %q (specific type must beat the generic Organization wrapper regardless of document order)", pick.category, "YogaStudio")
	}
	if pick.node["name"] != "Real Yoga Studio" {
		t.Errorf("picked node name = %v; want %q", pick.node["name"], "Real Yoga Studio")
	}
}

// TestPickJSONLDCategory_StructuralValueTypesNeverWin covers the 2026-09-25
// review fix: categoryTier defaulted every UNRECOGNIZED @type to tier 1
// (specific), so schema.org structural/value nodes that legitimately appear
// as top-level @graph members (PostalAddress, GeoCoordinates, ContactPoint,
// ...) or content families reachable only by suffix (DanceEvent, "Event")
// would outrank a real business @type sitting right next to them.
func TestPickJSONLDCategory_StructuralValueTypesNeverWin(t *testing.T) {
	cases := []struct {
		name  string
		nodes []map[string]any
		want  string
	}{
		{
			"PostalAddress top-level graph member loses to LocalBusiness",
			[]map[string]any{
				{"@type": "PostalAddress", "streetAddress": "1 Main St"},
				{"@type": "LocalBusiness", "name": "Acme"},
			},
			"LocalBusiness",
		},
		{
			"DanceEvent (suffix rule: ends in Event) loses to Organization",
			[]map[string]any{
				{"@type": "Organization", "name": "Studio Co"},
				{"@type": "DanceEvent", "name": "Friday Social"},
			},
			"Organization",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pick := pickJSONLDCategory(tc.nodes)
			if pick == nil {
				t.Fatalf("pickJSONLDCategory returned nil; want %q", tc.want)
			}
			if pick.category != tc.want {
				t.Errorf("category = %q; want %q", pick.category, tc.want)
			}
		})
	}
}

// TestExtractContacts_JSONLD_TypeArray_SpecificOverGeneric is a regression
// guard for the array-@type path specifically (as opposed to the
// multi-node @graph path above): a single node with @type
// ["LocalBusiness","ExerciseGym"] must still pick the specific member
// (ExerciseGym) even after the 2026-09-25 categoryTier review fix.
func TestExtractContacts_JSONLD_TypeArray_SpecificOverGeneric(t *testing.T) {
	html := `<html><head>
<script type="application/ld+json">
{"@type": ["LocalBusiness", "ExerciseGym"], "name": "Iron Works Gym"}
</script>
</head><body></body></html>`

	cd := ExtractContacts([]byte(html))
	if cd.BusinessCategory != "ExerciseGym" {
		t.Errorf("BusinessCategory = %q; want %q", cd.BusinessCategory, "ExerciseGym")
	}
}

// TestFlattenJSONLDNodes_NestedGraph verifies @graph arrays (including a
// nested array-of-arrays edge case some generators emit) are fully expanded
// to a flat list, and that a node with no @graph is passed through as-is.
func TestFlattenJSONLDNodes_NestedGraph(t *testing.T) {
	raw := []map[string]any{
		{
			"@context": "https://schema.org",
			"@graph": []any{
				map[string]any{"@type": "WebSite", "name": "A"},
				map[string]any{"@type": "HealthClub", "name": "B"},
			},
		},
		{"@type": "LocalBusiness", "name": "C"}, // no @graph — passthrough
	}
	got := flattenJSONLDNodes(raw)
	// Expect: the @graph container itself (no usable @type), WebSite, HealthClub, LocalBusiness = 4 nodes.
	if len(got) != 4 {
		t.Fatalf("flattenJSONLDNodes returned %d nodes; want 4 (container + 2 graph members + 1 passthrough): %+v", len(got), got)
	}
	var sawHealthClub, sawLocalBusiness bool
	for _, n := range got {
		if n["name"] == "B" && n["@type"] == "HealthClub" {
			sawHealthClub = true
		}
		if n["name"] == "C" && n["@type"] == "LocalBusiness" {
			sawLocalBusiness = true
		}
	}
	if !sawHealthClub {
		t.Error("flattened nodes missing the @graph-nested HealthClub node")
	}
	if !sawLocalBusiness {
		t.Error("flattened nodes missing the passthrough LocalBusiness node")
	}
}

// TestJSONLDTypes_ArrayAndPrefix verifies @type normalization: string form,
// array form, and the schema.org URI/prefix forms some generators emit.
func TestJSONLDTypes_ArrayAndPrefix(t *testing.T) {
	cases := []struct {
		name string
		ld   map[string]any
		want []string
	}{
		{"string", map[string]any{"@type": "HealthClub"}, []string{"HealthClub"}},
		{"array", map[string]any{"@type": []any{"LocalBusiness", "DaySpa"}}, []string{"LocalBusiness", "DaySpa"}},
		{"schema prefix", map[string]any{"@type": "schema:HealthClub"}, []string{"HealthClub"}},
		{"https URI", map[string]any{"@type": "https://schema.org/HealthClub"}, []string{"HealthClub"}},
		{"missing", map[string]any{}, nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := jsonLDTypes(tc.ld)
			if len(got) != len(tc.want) {
				t.Fatalf("jsonLDTypes(%+v) = %v; want %v", tc.ld, got, tc.want)
			}
			for i := range got {
				if got[i] != tc.want[i] {
					t.Errorf("jsonLDTypes(%+v)[%d] = %q; want %q", tc.ld, i, got[i], tc.want[i])
				}
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
