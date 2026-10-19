package prodir

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

const pmaFormURL = pmaBase + "?controller=memberDirectory&action=SearchForm&directory_id=5"

// pmaFakeFetcher serves the fixtures by URL; any other search answers "no matching records".
func pmaFakeFetcher(t *testing.T) *fakeFetcher {
	empty := readFixture(t, "pma_list_empty_SYNTHETIC.html")
	pages := map[string]Page{
		pmaFormURL: {Body: readFixture(t, "pma_form_SYNTHETIC.html")},
		pmaSearchURL("srch_COUNTRY_countryList_47", "US", 1):    {Body: readFixture(t, "pma_list_p1_SYNTHETIC.html")},
		pmaSearchURL("srch_COUNTRY_countryList_47", "US", 21):   {Body: readFixture(t, "pma_list_p2_SYNTHETIC.html")},
		pmaSearchURL("srch_LASTNAME_singleLineText_46", "a", 1): {Body: readFixture(t, "pma_list_prefix_SYNTHETIC.html")},
	}
	for _, c := range []string{"AU"} {
		pages[pmaSearchURL("srch_COUNTRY_countryList_47", c, 1)] = Page{Body: empty}
	}
	for c := 'b'; c <= 'z'; c++ {
		pages[pmaSearchURL("srch_LASTNAME_singleLineText_46", string(c), 1)] = Page{Body: empty}
	}
	return &fakeFetcher{pages: pages}
}

func TestPMAList(t *testing.T) {
	s := mustSource(t, "pma")
	f := pmaFakeFetcher(t)
	got := collect(t, s, f)
	det := func(id string) string {
		return pmaBase + "?controller=memberDirectory&action=resultsDetail&directory_id=5&detail_lookup_id=" + id + "&bCustomMapData=0"
	}
	want := []Record{
		{ID: "0a1b2c3d4e5f0001", URL: det("0A1B2C3D4E5F0001"), Name: "Ada Fixture", City: "Samplevale", Region: "CA", PostalCode: "90000", Country: "US",
			Emails: []string{"ada.fixture@example.com"}, Meta: map[string][]string{}},
		{ID: "0a1b2c3d4e5f0002", URL: det("0A1B2C3D4E5F0002"), Name: "Bo Placeholder", Business: "Fixture Pilates Studio", City: "Mockton", Region: "NC", PostalCode: "28277-1234",
			Country: "US", Emails: []string{"bo@example.org"}, Meta: map[string][]string{}},
		// a place line in another shape is kept whole in Address, never split into a guessed city
		{ID: "0a1b2c3d4e5f0003", URL: det("0A1B2C3D4E5F0003"), Name: "Cy Sample", Address: "Somewhere Odd 12345", Country: "US", Meta: map[string][]string{}},
		// found by the last-name sweep: no country on the row, none guessed
		{ID: "0a1b2c3d4e5f0004", URL: det("0A1B2C3D4E5F0004"), Name: "Di Blankcountry", Address: "Nowhereville", Emails: []string{"di@example.com"}, Meta: map[string][]string{}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// form, US p1, US p2 (start=21 since 25 results), AU, a..z
	if len(f.calls) != 1+2+1+26 {
		t.Errorf("%d requests: %v", len(f.calls), f.calls)
	}
	if !strings.Contains(f.calls[1], "srch_MEMBERTYPE_hidden_50=%27TCH%27") || !strings.Contains(f.calls[1], "srch_isDirectory_hidden_45=1") ||
		!strings.Contains(f.calls[1], "srch_COUNTRY_countryList_47=US") {
		t.Errorf("first search = %s (the US must come first, with the hidden criteria)", f.calls[1])
	}

	// the limit stops at once
	f = pmaFakeFetcher(t)
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); err == nil || len(f.calls) != 2 {
		t.Errorf("limit: err=%v requests=%d", err, len(f.calls))
	}
	// a form without countries is a layout error
	f = &fakeFetcher{pages: map[string]Page{pmaFormURL: {Body: []byte("<html></html>")}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("no country options must fail")
	}
}

func TestPMADetail(t *testing.T) {
	s := mustSource(t, "pma")
	r := Record{ID: "0a1b2c3d4e5f0001", Name: "Ada Fixture", Country: "US",
		URL: pmaBase + "?controller=memberDirectory&action=resultsDetail&directory_id=5&detail_lookup_id=0A1B2C3D4E5F0001&bCustomMapData=0"}

	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "pma_detail_full_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{Address: "104 Fixture Street", City: "Samplevale", Region: "CA", PostalCode: "90000",
		Emails: []string{"ada.fixture@example.com"}, Phones: []string{"555-010-0101"}, Websites: []string{"http://www.ada-fixture.example.org"},
		Bio: "Teaches mat Pilates since 2005."}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}

	// no phone, no website, empty "About Me", city line only
	f = &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "pma_detail_min_SYNTHETIC.html")}}}
	d, err = s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want = Record{City: "Samplevale", Region: "CA", PostalCode: "90000", Emails: []string{"ada.fixture@example.com"}}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("minimal detail = %#v\nwant            %#v", d, want)
	}
	// the merged record keeps the list's country
	if m := mergeRecord(r, d); m.Country != "US" || m.Name != "Ada Fixture" {
		t.Errorf("merged = %#v", m)
	}

	f = &fakeFetcher{pages: map[string]Page{r.URL: {Body: []byte("<html></html>")}}}
	if _, err := s.Detail(context.Background(), f, r); err == nil {
		t.Error("a page without member details must fail")
	}
}
