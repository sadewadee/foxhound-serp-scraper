package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestErepsList(t *testing.T) {
	s := mustSource(t, "ereps")
	u := func(n string) string { return erepsBase + "/member-directory?page=" + n }
	f := &fakeFetcher{pages: map[string]Page{
		u("0"): {Body: readFixture(t, "ereps_list_SYNTHETIC.html")},
		u("1"): {Body: readFixture(t, "ereps_list_empty_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "9001", URL: erepsBase + "/member-profile/9001", Name: "Alex Fixture", City: "Exampleton", Country: "Belgium",
			Meta: map[string][]string{
				"qualified_as":   {"Group Fitness Instructor", "Graduate Exercise Professional"},
				"master_trainer": {"Master Trainer"}, "expires": {"12/December/2099"}}},
		{ID: "9002", URL: erepsBase + "/member-profile/9002", Name: "Blair Placeholder", City: "Sampleford", Country: "Germany",
			Meta: map[string][]string{"qualified_as": {"Personal Trainer"}, "expires": {"01/January/2099"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 { // page 0, then the empty page 1 ends the walk
		t.Errorf("requests = %v", f.calls)
	}
	if err := s.List(context.Background(), &fakeFetcher{pages: map[string]Page{u("0"): {Status: 500}}}, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
}

func TestErepsDetail(t *testing.T) {
	s := mustSource(t, "ereps")
	r := Record{ID: "9001", URL: erepsBase + "/member-profile/9001"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "ereps_profile_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{Websites: []string{"http://alex-fixture.example.com"}, Bio: "Synthetic personal trainer. Reach me at fixture."}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}

	// no website and only the placeholder bio: nothing to add
	f.pages[r.URL] = Page{Body: readFixture(t, "ereps_profile_plain_SYNTHETIC.html")}
	if d, err := s.Detail(context.Background(), f, r); err != nil || !reflect.DeepEqual(d, Record{}) {
		t.Errorf("plain detail = %#v, %v", d, err)
	}

	// a profile without any section (the member filled in nothing) is valid and adds nothing
	f.pages[r.URL] = Page{Body: []byte(`<html><body><h1 class="page-title">Member Profile</h1></body></html>`)}
	if d, err := s.Detail(context.Background(), f, r); err != nil || !reflect.DeepEqual(d, Record{}) {
		t.Errorf("empty profile = %#v, %v", d, err)
	}

	// layout change and gone pages
	f.pages[r.URL] = Page{Body: []byte("<html><body>nothing</body></html>")}
	if _, err := s.Detail(context.Background(), f, r); err == nil {
		t.Error("layout change must fail")
	}
	f.pages[r.URL] = Page{Status: 404}
	if _, err := s.Detail(context.Background(), f, r); err == nil {
		t.Error("404 must return ErrNotFound")
	}

	// domain and category
	l := recordListing(s, mergeRecord(Record{ID: "9001", Name: "Alex Fixture", Country: "Belgium"}, want), newCountryResolver())
	if l.Category != "fitness_trainer" || l.CountryCode != "BE" || l.Domain != "9001.ereps.www.ereps.eu" {
		t.Errorf("listing = %+v", l)
	}
}
