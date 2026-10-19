package prodir

import (
	"errors"
	"reflect"
	"testing"
)

func TestLagreeList(t *testing.T) {
	s := mustSource(t, "lagree")
	f := &fakeFetcher{pages: map[string]Page{lagreeURL: {Body: readFixture(t, "lagree_locator_SYNTHETIC.html")}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "9001", Name: "Example Lagree Studio", Address: "100 Example St, Exampleville, CA 90001",
			Country: "United States", Region: "CA", Phones: []string{"555-0101"},
			Websites: []string{"https://example-lagree.example.com"}, Bio: "Contact us at hello@example.org",
			Social: map[string]string{}},
		{ID: "9002", Name: "Glide N' Shine Sample", Address: "5 Placeholder Rd, Sampletown, TX 78701",
			Country: "United States", Region: "TX", Websites: []string{"http://www.sample-shine.example.com"},
			Social: map[string]string{}},
		{ID: "9003", Name: "Paris Synthetic Lagree", Address: "1 Rue Exemple 75001 Paris, France",
			Country: "France", Phones: []string{"+33 555 0102"},
			Social: map[string]string{"instagram": "https://www.instagram.com/synthetic_paris/"}},
		{ID: "9004", Name: "Jeddah Fixture Studio", Address: "Fixture District, Jeddah Saudi Arabia",
			Country: "Saudi Arabia", Social: map[string]string{}},
		// "7 Unknown Way, Faketown, CA": a bare "CA" is not a country and has no ZIP
		{ID: "9006", Name: "Nowhere Studio", Address: "7 Unknown Way, Faketown, CA", Social: map[string]string{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 1 {
		t.Errorf("requests = %v, want the single locator GET", f.calls)
	}
	// the default category and listing type apply
	if s.Category != "pilates_instructor" || s.ListingType != "studio" {
		t.Errorf("category=%q listing=%q", s.Category, s.ListingType)
	}

	// the -limit error stops the walk
	err := s.List(t.Context(), f, func(Record) error { return errListLimit })
	if !errors.Is(err, errListLimit) {
		t.Errorf("limit: %v", err)
	}
	// a page without the array is a layout change, not an empty directory
	f = &fakeFetcher{pages: map[string]Page{lagreeURL: {Body: []byte("<html>nothing</html>")}}}
	if err := s.List(t.Context(), f, func(Record) error { return nil }); err == nil {
		t.Error("missing stores array must fail List")
	}
	f = &fakeFetcher{pages: map[string]Page{lagreeURL: {Status: 500}}}
	if err := s.List(t.Context(), f, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
}

func TestLagreeListing(t *testing.T) {
	s := mustSource(t, "lagree")
	l := recordListing(s, Record{ID: "9001", Name: "Example Lagree Studio", Country: "United States",
		Websites: []string{"https://example-lagree.example.com"}}, newCountryResolver())
	if l.Country != "United States" || l.CountryCode != "US" || l.Niche != "pilates" {
		t.Errorf("listing country=%q/%q niche=%q", l.Country, l.CountryCode, l.Niche)
	}
}
