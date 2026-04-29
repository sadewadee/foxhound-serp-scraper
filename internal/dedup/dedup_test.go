package dedup

import (
	"testing"
)

// Comprehensive table-driven test for CanonicalURL — ensures tracking-param
// stripping works without breaking functional params, regression-tests the
// production msockid case, and exercises every category in trackingParams +
// trackingParamPrefixes.
func TestCanonicalURL(t *testing.T) {
	tests := []struct {
		name string
		in   string
		want string
	}{
		// === Baseline cases (no change expected) ===
		{
			name: "no query params unchanged",
			in:   "https://example.com/page",
			want: "https://example.com/page",
		},
		{
			name: "empty path becomes /",
			in:   "https://example.com",
			want: "https://example.com/",
		},
		{
			name: "fragment stripped",
			in:   "https://example.com/page#section",
			want: "https://example.com/page",
		},
		{
			name: "scheme and host lowercased",
			in:   "HTTPS://EXAMPLE.COM/page",
			want: "https://example.com/page",
		},
		{
			name: "trailing slash stripped",
			in:   "https://example.com/page/",
			want: "https://example.com/page",
		},
		{
			name: "functional params preserved (q, page, lang, hl, v, id)",
			in:   "https://example.com/search?q=hello&page=2&lang=en&hl=en&v=abc&id=42",
			want: "https://example.com/search?hl=en&id=42&lang=en&page=2&q=hello&v=abc",
		},

		// === Bing tracking (PRIMARY production case) ===
		{
			name: "msockid stripped",
			in:   "https://www.nike.com/hr/a/what-is-hiit-workout?msockid=388b8202b32061d635f99548b23e6087",
			want: "https://www.nike.com/hr/a/what-is-hiit-workout",
		},
		{
			name: "msockid stripped + functional preserved",
			in:   "https://example.com/page?msockid=abc&q=fitness&page=2",
			want: "https://example.com/page?page=2&q=fitness",
		},
		{
			name: "regression: two URLs differing only in msockid → same hash",
			in:   "https://www.nike.com/hr/a/what-is-hiit-workout?msockid=AAA",
			want: "https://www.nike.com/hr/a/what-is-hiit-workout",
		},

		// === Google tracking ===
		{
			name: "gclid stripped",
			in:   "https://example.com/?gclid=abc123",
			want: "https://example.com/",
		},
		{
			name: "ved + ei + sxsrf stripped",
			in:   "https://example.com/?ved=A&ei=B&sxsrf=C",
			want: "https://example.com/",
		},

		// === UTM prefix ===
		{
			name: "utm_* family stripped",
			in:   "https://example.com/?utm_source=fb&utm_medium=cpc&utm_campaign=spring&utm_term=fit&utm_content=adA",
			want: "https://example.com/",
		},
		{
			name: "utm mixed with functional",
			in:   "https://example.com/?utm_source=fb&q=hello&utm_medium=cpc",
			want: "https://example.com/?q=hello",
		},

		// === Facebook ===
		{
			name: "fbclid stripped",
			in:   "https://example.com/?fbclid=abc",
			want: "https://example.com/",
		},
		{
			name: "facebook __tn_ + __xts__ + __cft__ prefixes stripped",
			in:   "https://example.com/?__tn__=H&__xts__[0]=foo&__cft__[0]=bar",
			want: "https://example.com/",
		},

		// === Mailchimp prefix ===
		{
			name: "mc_* mailchimp stripped",
			in:   "https://example.com/?mc_cid=abc&mc_eid=def",
			want: "https://example.com/",
		},

		// === HubSpot prefix ===
		{
			name: "_hs* hubspot stripped",
			in:   "https://example.com/?_hsenc=A&_hsmi=B",
			want: "https://example.com/",
		},

		// === Amazon prefix ===
		{
			name: "pf_rd_* amazon stripped",
			in:   "https://www.amazon.de/product?pf_rd_p=A&pf_rd_r=B&pf_rd_t=C",
			want: "https://www.amazon.de/product",
		},

		// === Microsoft Ads ===
		{
			name: "msclkid stripped",
			in:   "https://example.com/?msclkid=abc",
			want: "https://example.com/",
		},

		// === Yandex ===
		{
			name: "yclid + _openstat stripped",
			in:   "https://example.com/?yclid=abc&_openstat=def",
			want: "https://example.com/",
		},

		// === Twitter — KEEP `s` and `t` (ambiguous) ===
		{
			name: "twclid stripped but s/t preserved",
			in:   "https://twitter.com/x?twclid=abc&s=09&t=hello",
			want: "https://twitter.com/x?s=09&t=hello",
		},

		// === LinkedIn ===
		{
			name: "trk + lipi + trackingId stripped",
			in:   "https://www.linkedin.com/in/alice?trk=A&lipi=B&trackingId=C",
			want: "https://www.linkedin.com/in/alice",
		},

		// === Reddit ===
		{
			name: "share_id + correlation_id + rdt stripped",
			in:   "https://reddit.com/r/x?share_id=A&correlation_id=B&rdt=C",
			want: "https://reddit.com/r/x",
		},

		// === Yelp filter params (functional) ===
		{
			name: "Yelp find_loc/find_desc/cflt preserved",
			in:   "https://www.yelp.com/search?find_loc=NYC&find_desc=gym&cflt=fitness",
			want: "https://www.yelp.com/search?cflt=fitness&find_desc=gym&find_loc=NYC",
		},

		// === Mixed real-world ===
		{
			name: "real Bing search-result URL with msockid + utm",
			in:   "https://www.example.com/blog/post?msockid=abc&utm_source=bing&utm_medium=cpc&page=2",
			want: "https://www.example.com/blog/post?page=2",
		},
		{
			name: "GA tracking _ga + _gl stripped",
			in:   "https://example.com/?_ga=2.1.234.567&_gl=1*ab*cd",
			want: "https://example.com/",
		},

		// === Edge cases ===
		{
			name: "malformed URL fallback to lowercase",
			in:   "::not a url",
			want: "::not a url",
		},
		{
			name: "URL with port preserved",
			in:   "https://example.com:8080/page?msockid=A",
			want: "https://example.com:8080/page",
		},
		{
			name: "URL with userinfo preserved",
			in:   "https://user:pass@example.com/page?msockid=A",
			want: "https://user:pass@example.com/page",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := CanonicalURL(tt.in)
			if got != tt.want {
				t.Errorf("CanonicalURL(%q)\n  got:  %q\n  want: %q", tt.in, got, tt.want)
			}
		})
	}
}

// TestHashURL_DedupesViaCanonical verifies the production scenario: same
// logical URL with different msockid values must produce the same SHA-256
// hash so the SERP-result-INSERT trigger's ON CONFLICT(url_hash) actually
// fires and dedupes.
func TestHashURL_DedupesViaCanonical(t *testing.T) {
	url1 := "https://www.nike.com/hr/a/what-is-hiit-workout?msockid=388b8202b32061d635f99548b23e6087"
	url2 := "https://www.nike.com/hr/a/what-is-hiit-workout?msockid=0b31c8c8341f67230af5df82352d6620"
	url3 := "https://www.nike.com/hr/a/what-is-hiit-workout?msockid=1da96011c46968661cd2775bc51969e7"
	clean := "https://www.nike.com/hr/a/what-is-hiit-workout"

	h1 := HashURL(url1)
	h2 := HashURL(url2)
	h3 := HashURL(url3)
	hClean := HashURL(clean)

	if h1 != h2 || h2 != h3 || h3 != hClean {
		t.Errorf("expected all four hashes equal after msockid strip:\n"+
			"  h1     (msockid=...87)  = %s\n"+
			"  h2     (msockid=...20)  = %s\n"+
			"  h3     (msockid=...e7)  = %s\n"+
			"  hClean (no msockid)     = %s",
			h1, h2, h3, hClean)
	}
}

// TestHashURL_DifferentResources verifies dedup does NOT collide across
// genuinely different URLs (different paths, different functional params).
func TestHashURL_DifferentResources(t *testing.T) {
	urls := []string{
		"https://example.com/page-a",
		"https://example.com/page-b",
		"https://example.com/page-a?q=hello",
		"https://example.com/page-a?q=world",
		"https://example.com/page-a?page=1",
		"https://example.com/page-a?page=2",
	}
	seen := make(map[string]string, len(urls))
	for _, u := range urls {
		h := HashURL(u)
		if prev, ok := seen[h]; ok {
			t.Errorf("hash collision: %q and %q both → %s", prev, u, h)
		}
		seen[h] = u
	}
}

// TestExtractDomain regression — change unrelated to tracking-strip but
// verifies domain parsing still works (used in SERP filter loop).
func TestExtractDomain(t *testing.T) {
	cases := []struct {
		in, want string
	}{
		{"https://www.zhihu.com/question/123", "www.zhihu.com"},
		{"https://Example.COM/page", "example.com"},
		{"https://www.amazon.de:443/foo", "www.amazon.de"},
		{"::garbage::", ""}, // unparseable → empty
	}
	for _, c := range cases {
		got := ExtractDomain(c.in)
		if got != c.want {
			t.Errorf("ExtractDomain(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
