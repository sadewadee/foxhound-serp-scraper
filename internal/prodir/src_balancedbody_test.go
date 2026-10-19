package prodir

import (
	"errors"
	"reflect"
	"testing"
)

func TestBalancedBodyList(t *testing.T) {
	s := mustSource(t, "balancedbody")
	pd := balancedbodyBase + "/page-data/studio-finder/page-data.json"
	sq := func(h string) string { return balancedbodyBase + "/page-data/sq/d/" + h + ".json" }
	f := &fakeFetcher{pages: map[string]Page{
		pd:               {Body: readFixture(t, "balancedbody_page-data_SYNTHETIC.json")},
		sq("1111111111"): {Body: readFixture(t, "balancedbody_sq_small_SYNTHETIC.json")},
		sq("2222222222"): {Body: readFixture(t, "balancedbody_sq_studios_SYNTHETIC.json")},
		sq("3333333333"): {Body: []byte("{}")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "SynthId1AAA", Name: "Example Reformer Studio", Address: "1 Example Road, Unit 2", City: "Exampleville",
			Region: "California", PostalCode: "90001", Country: "UNITED STATES", Emails: []string{"studio@example.com"},
			Phones: []string{"555-0101"}, Websites: []string{"https://example-reformer.example.com"},
			Meta: map[string][]string{"contact_name": {"Alex Fixture"}, "equipment": {"Studio Reformer", "Allegro 2 Reformer", "Tower"}}},
		// approved: null counts as approved; scheme-less website gets http://
		{ID: "synthid2bbb", Name: "Sample Pilates Co", Address: "5 Sample Lane", City: "Sampleton",
			PostalCode: "SA1 1AA", Country: "United Kingdom", Emails: []string{"hello@example.org"},
			Websites: []string{"http://www.sample-pilates.example.org"}, Meta: map[string][]string{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// page data + the hashes up to the one holding the studios (not the third)
	if len(f.calls) != 3 || f.calls[0] != pd || f.calls[2] != sq("2222222222") {
		t.Errorf("requests = %v", f.calls)
	}

	// a rebuild changes the hash: it is read from the page data every run
	f = &fakeFetcher{pages: map[string]Page{
		pd:        {Body: []byte(`{"staticQueryHashes":["999"]}`)},
		sq("999"): {Body: readFixture(t, "balancedbody_sq_studios_SYNTHETIC.json")},
	}}
	if got := collect(t, s, f); len(got) != 2 {
		t.Errorf("rebuilt hash: %d records", len(got))
	}

	// no query with the studios, a missing page and the -limit
	f = &fakeFetcher{pages: map[string]Page{pd: {Body: []byte(`{"staticQueryHashes":["1"]}`)}, sq("1"): {Body: []byte("{}")}}}
	if err := s.List(t.Context(), f, func(Record) error { return nil }); err == nil {
		t.Error("no studios query must fail List")
	}
	if err := s.List(t.Context(), &fakeFetcher{}, func(Record) error { return nil }); err == nil {
		t.Error("missing page-data must fail List")
	}
	f = &fakeFetcher{pages: map[string]Page{
		pd:        {Body: []byte(`{"staticQueryHashes":["999"]}`)},
		sq("999"): {Body: readFixture(t, "balancedbody_sq_studios_SYNTHETIC.json")},
	}}
	if err := s.List(t.Context(), f, func(Record) error { return errListLimit }); !errors.Is(err, errListLimit) {
		t.Errorf("limit: %v", err)
	}
}

func TestBalancedBodyListing(t *testing.T) {
	s := mustSource(t, "balancedbody")
	if s.ListingType != "studio" {
		t.Errorf("listing type = %q", s.ListingType)
	}
	l := recordListing(s, Record{ID: "SynthId1AAA", Name: "Example Reformer Studio", Country: "UNITED STATES"}, newCountryResolver())
	if l.CountryCode != "US" || l.Niche != "pilates" {
		t.Errorf("country=%q niche=%q", l.CountryCode, l.Niche)
	}
}

func TestBalancedBodyCountry(t *testing.T) {
	res := newCountryResolver()
	for in, want := range map[string]string{
		"UNITED STATES": "US", "United Kingdom": "GB", "Italia": "IT", "Schweiz": "CH", " Ελλάδα ": "GR", "美国": "US", "Nowhereland": "",
	} {
		if _, code := res.resolve(balancedbodyCountry(in)); code != want {
			t.Errorf("country %q -> %q, want %q", in, code, want)
		}
	}
}
