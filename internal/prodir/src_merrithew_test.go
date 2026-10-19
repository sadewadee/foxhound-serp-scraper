package prodir

import (
	"context"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// merrithewFake answers the API by its query: the first US page, the second,
// and "no results" for every other country group.
type merrithewFake struct {
	p1, p2, empty []byte
	queries       []url.Values
	hdr           map[string]string
}

func (f *merrithewFake) Get(_ context.Context, u string, hdr map[string]string) (Page, error) {
	f.hdr = hdr
	pu, _ := url.Parse(u)
	q := pu.Query()
	f.queries = append(f.queries, q)
	body := f.empty
	if reflect.DeepEqual(q["countryCodes[]"], []string{"US"}) {
		body = f.p1
		if q.Get("skip") == "1000" {
			body = f.p2
		}
	}
	return Page{Status: 200, URL: u, Body: body}, nil
}

func (f *merrithewFake) Post(ctx context.Context, u string, h map[string]string, _ []byte) (Page, error) {
	return f.Get(ctx, u, h)
}

func TestMerrithewList(t *testing.T) {
	s := mustSource(t, "merrithew")
	f := &merrithewFake{
		p1: readFixture(t, "merrithew_p1_SYNTHETIC.json"), p2: readFixture(t, "merrithew_p2_SYNTHETIC.json"),
		empty: readFixture(t, "merrithew_empty_SYNTHETIC.json"),
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "900001", Name: "Ada Fixture", City: "Samplevale", Region: "Examplestate", Country: "United States",
			Emails: []string{"ada.fixture@example.com"}, Phones: []string{"555-010-0101"},
			Meta: map[string][]string{"certifications": {"STOTT PILATES® Matwork", "Total Barre®"}, "current_specialist": {"yes"}}},
		{ID: "900002", Name: "Bo Placeholder", City: "Mockton", Country: "United States", Emails: []string{"bo@example.org"}, Meta: map[string][]string{}},
		{ID: "900004", Name: "Cy Sample", City: "Fakeham", Region: "Ontario", Country: "Canada", Meta: map[string][]string{"certifications": {"CORE™"}}},
	}
	if !reflect.DeepEqual(got, want) { // the nameless row and the repeat of Bo on page 2 are dropped; bad email / "n/a" phone ignored
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if f.hdr["Accept"] != "application/vnd.api.v3+json" {
		t.Errorf("headers = %v", f.hdr)
	}

	// US is paged by skip (0, 1000); then CA, GB, AU alone; the rest ten codes per query
	var shape []string
	covered := map[string]bool{}
	for _, q := range f.queries {
		shape = append(shape, strings.Join(q["countryCodes[]"], ",")+"@"+q.Get("skip"))
		for _, c := range q["countryCodes[]"] {
			covered[c] = true
		}
		if q.Get("take") != "1000" || q.Get("certification") != "0" {
			t.Errorf("query params = %v", q)
		}
	}
	if shape[0] != "US@0" || shape[1] != "US@1000" || shape[2] != "CA@0" || shape[3] != "GB@0" || shape[4] != "AU@0" {
		t.Errorf("first queries = %v", shape[:5])
	}
	if n := len(f.queries); n < 20 || n > 40 {
		t.Errorf("%d queries, want roughly 30", n)
	}
	for _, c := range []string{"FR", "DE", "JP", "BR", "ZA", "NZ", "SG", "MX"} {
		if !covered[c] {
			t.Errorf("country %s never queried", c)
		}
	}

	// emit's error (the -limit) stops at once; an HTTP error or bad JSON fails the stage
	f2 := &merrithewFake{p1: f.p1, p2: f.p2, empty: f.empty}
	if err := s.List(context.Background(), f2, func(Record) error { return errListLimit }); err == nil || len(f2.queries) != 1 {
		t.Errorf("limit: err=%v queries=%d", err, len(f2.queries))
	}
	bad := &merrithewFake{p1: []byte("<html>"), empty: []byte("<html>")}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("a non-JSON answer must fail List")
	}
}

func TestMerrithewListing(t *testing.T) {
	s := mustSource(t, "merrithew")
	r := Record{ID: "900001", Name: "Ada Fixture", City: "Samplevale", Country: "United States", Emails: []string{"ada.fixture@example.com"}, Phones: []string{"555-010-0101"}}
	l := recordListing(s, r, newCountryResolver())
	if l.Domain != "900001.merrithew.merrithew.com" || l.CountryCode != "US" || l.Category != "pilates_instructor" || l.Niche != "pilates" ||
		!reflect.DeepEqual(l.Emails, []string{"ada.fixture@example.com"}) {
		t.Errorf("listing = %#v", l)
	}
}
