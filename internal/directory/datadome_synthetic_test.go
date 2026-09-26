package directory

import (
	"os"
	"strings"
	"testing"
)

// The fixtures in this file are SYNTHETIC: invented markup modeled on the
// sites' documented public structure, validated here only as a contract for
// the selectors. Yelp and TripAdvisor return DataDome 403s to every method
// available today, so there is deliberately no live-verified behavior. The
// "To verify on activation" checklist in docs/directory-datadome.md lists the
// exact checks to run against real pages when the module is switched on.

func loadSynthetic(t *testing.T, name string) []byte {
	t.Helper()
	data, err := os.ReadFile("testdata/" + name)
	if err != nil {
		t.Fatalf("read %s: %v", name, err)
	}
	return data
}

func TestYelpExtractor_SyntheticFixture(t *testing.T) {
	listings := (&YelpExtractor{}).Extract(loadSynthetic(t, "yelp_search_SYNTHETIC.html"))

	if len(listings) != 2 {
		t.Fatalf("got %d listings, want 2", len(listings))
	}

	first := listings[0]
	if first.Name != "Day Spa Honolulu" {
		t.Errorf("name = %q", first.Name)
	}
	if first.URL != "https://dayspahonolulu.example/" {
		t.Errorf("URL = %q, want the unwrapped business website, never a yelp.com URL", first.URL)
	}
	if !strings.Contains(first.Address, "Honolulu") {
		t.Errorf("address = %q", first.Address)
	}
	if first.Category != "Day Spas" {
		t.Errorf("category = %q", first.Category)
	}
	if second := listings[1]; second.URL != "" {
		t.Errorf("URL = %q for a business with no website, want empty so nothing is queued", second.URL)
	}
}

func TestTripAdvisorExtractor_SyntheticFixture(t *testing.T) {
	listings := (&TripAdvisorExtractor{}).Extract(loadSynthetic(t, "tripadvisor_search_SYNTHETIC.html"))

	if len(listings) != 2 {
		t.Fatalf("got %d listings, want 2", len(listings))
	}

	first := listings[0]
	if first.Name != "Spice Garden Ubud" {
		t.Errorf("name = %q", first.Name)
	}
	if first.URL != "https://spicegarden.example/" {
		t.Errorf("URL = %q, want the business website, never a tripadvisor.com URL", first.URL)
	}
	if !strings.Contains(first.Address, "Ubud") {
		t.Errorf("address = %q", first.Address)
	}
	if second := listings[1]; second.URL != "" {
		t.Errorf("URL = %q for a business with no website, want empty so nothing is queued", second.URL)
	}
}

func TestDataDomeExtractors_NeverQueueDirectoryURL(t *testing.T) {
	for name, ext := range map[string]Extractor{"yelp": &YelpExtractor{}, "tripadvisor": &TripAdvisorExtractor{}} {
		for _, l := range ext.Extract([]byte("<html><body></body></html>")) {
			if strings.Contains(l.URL, "yelp.com") || strings.Contains(l.URL, "tripadvisor.com") {
				t.Errorf("%s listing %q carries a directory URL %q", name, l.Name, l.URL)
			}
		}
		if got := businessWebsite("https://www."+name+".com/biz/x", name+".com"); got != "" {
			t.Errorf("%s: businessWebsite returned %q for an internal URL", name, got)
		}
	}
}
