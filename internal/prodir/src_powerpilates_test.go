package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestPowerPilatesList(t *testing.T) {
	s := mustSource(t, "powerpilates")
	u := func(c string, n string) string { return ppBase + "/instructors/" + c + "/?page=" + n }
	f := &fakeFetcher{pages: map[string]Page{
		ppBase + "/instructors/": {Body: readFixture(t, "powerpilates_index_SYNTHETIC.html")},
		u("exampleland", "1"):    {Body: readFixture(t, "powerpilates_country_p1_SYNTHETIC.html")},
		u("exampleland", "2"):    {Body: readFixture(t, "powerpilates_country_p2_SYNTHETIC.html")},
		u("testonia", "1"):       {Body: readFixture(t, "powerpilates_country_empty_SYNTHETIC.html")},
		u("exampleland", "3"):    {Status: 500}, // must never be asked for: page 2 has no next link
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "ada-fixture", URL: ppBase + "/instructors/profile/ada-fixture/", Name: "ADA FIXTURE", City: "Samplevale", Country: "Exampleland",
			Meta: map[string][]string{"certification_level": {"Complete Apparatus"}}},
		{ID: "bo-placeholder", URL: ppBase + "/instructors/profile/bo%20placeholder/", Name: "Bo Placeholder", City: "Mockton", Country: "Exampleland",
			Meta: map[string][]string{"certification_level": {"Mat"}}},
		{ID: "cy-sample", URL: ppBase + "/instructors/profile/cy-sample/", Name: "Cy Sample", City: "Fakeham", Country: "Exampleland",
			Meta: map[string][]string{"certification_level": {"Reformer"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 4 { // index, 2 pages of Exampleland, the empty Testonia
		t.Errorf("requests = %v", f.calls)
	}

	// emit's error (the -limit) stops at once
	f2 := &fakeFetcher{pages: f.pages}
	if err := s.List(context.Background(), f2, func(Record) error { return errListLimit }); err == nil || len(f2.calls) != 2 {
		t.Errorf("limit: err=%v requests=%v", err, f2.calls)
	}
	// a server error fails the stage
	if err := s.List(context.Background(), &fakeFetcher{pages: map[string]Page{ppBase + "/instructors/": {Status: 500}}},
		func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
}

func TestPowerPilatesDetail(t *testing.T) {
	s := mustSource(t, "powerpilates")
	r := Record{ID: "ada-fixture", URL: ppBase + "/instructors/profile/ada-fixture/", Name: "ADA FIXTURE", Country: "Exampleland"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "powerpilates_profile_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	wantMeta := map[string][]string{
		"certification_level": {"Master Teacher Trainer"}, "time_certified": {"12 years, 3 months"}, "level_achieved": {"March 2014"},
		"expires": {"December 31, 2030"}, "certificate_number": {"00000001-555"}, "title": {"Certified Classical Pilates Instructor"},
	}
	if !reflect.DeepEqual(d.Meta, wantMeta) {
		t.Errorf("meta = %#v", d.Meta)
	}
	if d.Region != "EX" {
		t.Errorf("region = %q", d.Region)
	}
	if !reflect.DeepEqual(d.Social, map[string]string{"instagram": "https://instagram.com/ada.fixture.pilates"}) {
		t.Errorf("social = %#v (brand accounts must be dropped)", d.Social)
	}
	if d.Bio != "Ada teaches in Samplevale. IG @ada.fixture.pilates Web: www.ada-fixture-pilates.example.com Second paragraph." {
		t.Errorf("bio = %q", d.Bio)
	}
	if len(d.Emails)+len(d.Phones)+len(d.Websites) != 0 {
		t.Error("the registry has no structured contact fields")
	}

	// the loader finds the website and handle typed into the bio
	l := recordListing(s, mergeRecord(r, d), newCountryResolver())
	if l.Domain != "ada-fixture.powerpilates.powerpilates.com" {
		t.Errorf("listing domain = %q", l.Domain)
	}

	// a gone profile is ErrNotFound; an unrecognised page is an error
	if _, err := s.Detail(context.Background(), &fakeFetcher{}, r); err == nil {
		t.Error("404 must fail")
	}
	f = &fakeFetcher{pages: map[string]Page{r.URL: {Body: []byte("<html><body>maintenance</body></html>")}}}
	if _, err := s.Detail(context.Background(), f, r); err == nil {
		t.Error("a page without a profile must fail")
	}
}
