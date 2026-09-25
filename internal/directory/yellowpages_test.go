package directory

import (
	"os"
	"strings"
	"testing"
)

func TestYellowPagesExtractor_Match(t *testing.T) {
	e := &YellowPagesExtractor{}

	tests := []struct {
		domain string
		want   bool
	}{
		{"yellowpages.com", true},
		{"www.yellowpages.com", true},
		{"m.yellowpages.com", true},
		{"sub.domain.yellowpages.com", true},
		{"notyellowpages.com", false},
		{"google.com", false},
		{"yelp.com", false},
	}

	for _, tt := range tests {
		if got := e.Match(tt.domain); got != tt.want {
			t.Errorf("Match(%q) = %v; want %v", tt.domain, got, tt.want)
		}
	}
}

func TestYellowPagesExtractor_ExtractTrimmedFixture(t *testing.T) {
	data, err := os.ReadFile("testdata/yellowpages_search.html")
	if err != nil {
		t.Fatalf("failed to read testdata/yellowpages_search.html: %v", err)
	}

	e := &YellowPagesExtractor{}
	listings := e.Extract(data)

	// In the old implementation with selector ".result, .search-results .srp-listing, .v-card",
	// 4 cards would yield 12 duplicates. Now it must be exactly 4 unique listings.
	if len(listings) != 4 {
		t.Fatalf("expected 4 unique listings, got %d", len(listings))
	}

	// Listing 1: Honolulu Medspa with external website
	l1 := listings[0]
	if l1.Name != "Honolulu Medspa" {
		t.Errorf("listing 0 name = %q; want %q", l1.Name, "Honolulu Medspa")
	}
	if l1.URL != "http://honolulumedspa.com" {
		t.Errorf("listing 0 URL = %q; want %q", l1.URL, "http://honolulumedspa.com")
	}
	if l1.Phone != "(808) 528-0888" {
		t.Errorf("listing 0 Phone = %q; want %q", l1.Phone, "(808) 528-0888")
	}
	if !strings.Contains(l1.Address, "Honolulu") {
		t.Errorf("listing 0 Address = %q; expected to contain 'Honolulu'", l1.Address)
	}
	if l1.Category != "Day Spas" {
		t.Errorf("listing 0 Category = %q; want %q", l1.Category, "Day Spas")
	}

	// Listing 2: Touch Mini Day Spa with external website
	l2 := listings[1]
	if l2.Name != "Touch Mini Day Spa" {
		t.Errorf("listing 1 name = %q; want %q", l2.Name, "Touch Mini Day Spa")
	}
	if l2.URL != "http://touchminidayspa.localsearch.com" {
		t.Errorf("listing 1 URL = %q; want %q", l2.URL, "http://touchminidayspa.localsearch.com")
	}
	if l2.Phone != "(808) 732-5456" {
		t.Errorf("listing 1 Phone = %q; want %q", l2.Phone, "(808) 732-5456")
	}

	// Listing 3: Loess Spa has NO website -> URL must be empty (not YellowPages detail URL)
	l3 := listings[2]
	if l3.Name != "Loess Spa" {
		t.Errorf("listing 2 name = %q; want %q", l3.Name, "Loess Spa")
	}
	if l3.URL != "" {
		t.Errorf("listing 2 URL = %q; want empty string (business has no website)", l3.URL)
	}
	if l3.Phone != "(808) 841-3311" {
		t.Errorf("listing 2 Phone = %q; want %q", l3.Phone, "(808) 841-3311")
	}

	// Listing 4: Beautymed Therapy has internal yellowpages URL in website link -> URL must be empty
	l4 := listings[3]
	if l4.Name != "Beautymed Therapy" {
		t.Errorf("listing 3 name = %q; want %q", l4.Name, "Beautymed Therapy")
	}
	if l4.URL != "" {
		t.Errorf("listing 3 URL = %q; want empty string (must not queue internal yellowpages URL)", l4.URL)
	}
}

func TestYellowPagesExtractor_UnwrapRedirect(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"http://example.com/spa", "http://example.com/spa"},
		{"https://www.yellowpages.com/r?target=http%3A%2F%2Fspa.com", "http://spa.com"},
		{"https://www.yellowpages.com/redirect?dest=https%3A%2F%2Fexternal.com", "https://external.com"},
		{"http://yellowpages.com/honolulu-hi/mip/spa-123", "http://yellowpages.com/honolulu-hi/mip/spa-123"},
	}

	for _, tt := range tests {
		got := unwrapYellowPagesURL(tt.in)
		if got != tt.want {
			t.Errorf("unwrapYellowPagesURL(%q) = %q; want %q", tt.in, got, tt.want)
		}
	}
}

func TestYellowPagesExtractor_NoYellowPagesQueued(t *testing.T) {
	data, err := os.ReadFile("testdata/yellowpages_search.html")
	if err != nil {
		t.Fatalf("failed to read testdata/yellowpages_search.html: %v", err)
	}

	e := &YellowPagesExtractor{}
	listings := e.Extract(data)

	for _, l := range listings {
		if strings.Contains(l.URL, "yellowpages.com") {
			t.Errorf("listing %q has yellowpages.com in URL: %q; expected only external website or empty", l.Name, l.URL)
		}
	}
}

func TestYellowPagesExtractor_RealPage30Listings(t *testing.T) {
	data, err := os.ReadFile("testdata/yp_search_30listings.html")
	if err != nil {
		t.Fatalf("failed to read testdata/yp_search_30listings.html: %v", err)
	}

	e := &YellowPagesExtractor{}
	listings := e.Extract(data)
	if len(listings) != 30 {
		t.Errorf("expected 30 unique listings from full page, got %d", len(listings))
	}

	for _, l := range listings {
		if strings.Contains(l.URL, "yellowpages.com") {
			t.Errorf("listing %q has internal yellowpages URL %q", l.Name, l.URL)
		}
	}
}
