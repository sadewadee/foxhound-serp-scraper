package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestStrongfirstList(t *testing.T) {
	s := mustSource(t, "strongfirst")
	f := &fakeFetcher{pages: map[string]Page{strongfirstListURL: {Body: readFixture(t, "strongfirst_list_SYNTHETIC.json")}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "001aaaaaaaaaaaaaa1", URL: strongfirstBase + "/instructors/united-states/alex-fixture.001AAAAAAAAAAAAAA1/",
			Name: "Alex Fixture", City: "Exampleton", Country: "united states",
			Meta: map[string][]string{"level": {"Elite", "Kettlebell II", "Barbell"}, "availability": {"accepting new students", "online training"}}},
		{ID: "001aaaaaaaaaaaaaa2", URL: strongfirstBase + "/instructors/italy/blair-placeholder.001AAAAAAAAAAAAAA2/",
			Name: "Blair Placeholder", Country: "italy", Meta: map[string][]string{"level": {"Kettlebell I"}}},
		{ID: "001aaaaaaaaaaaaaa3", URL: strongfirstBase + "/instructors/korea,-republic-of/cameron-overseas.001AAAAAAAAAAAAAA3/",
			Name: "Cameron Overseas", City: "Seoul", Country: "Republic of Korea",
			Meta: map[string][]string{"level": {"Bodyweight"}, "availability": {"accepting new students"}}},
	}
	if !reflect.DeepEqual(got, want) { // duplicate id and blank name are dropped
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 1 {
		t.Errorf("requests = %v", f.calls)
	}

	// the slug country must resolve, "Korea, Republic of" included
	res := newCountryResolver()
	for i, code := range []string{"US", "IT", "KR"} {
		if l := recordListing(s, got[i], res); l.CountryCode != code {
			t.Errorf("%s: country %q -> %q, want %q", got[i].ID, got[i].Country, l.CountryCode, code)
		}
	}

	bad := &fakeFetcher{pages: map[string]Page{strongfirstListURL: {Body: []byte("<html>blocked</html>")}}}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("a non-JSON body must fail List")
	}
}

func TestStrongfirstDetail(t *testing.T) {
	s := mustSource(t, "strongfirst")
	r := Record{ID: "001aaaaaaaaaaaaaa1", URL: strongfirstBase + "/instructors/united-states/alex-fixture.001AAAAAAAAAAAAAA1/"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "strongfirst_profile_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{
		City: "Exampleton", PostalCode: "75000", Region: "Texas", Country: "United States",
		Websites: []string{"https://alex-fixture.example.com"},
		Bio:      "Synthetic strength coach. Write to alex@example.com.",
		Meta:     map[string][]string{"certification": {"StrongFirst Elite", "Kettlebell Instructor SFG II"}},
	}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}
	// the email in the bio is found by the loader, not stored as structured
	if l := recordListing(s, mergeRecord(r, d), newCountryResolver()); len(l.Emails) != 1 || l.CountryCode != "US" {
		t.Errorf("emails=%v country=%q", l.Emails, l.CountryCode)
	}

	// no website, empty zip
	f.pages[r.URL] = Page{Body: readFixture(t, "strongfirst_profile_plain_SYNTHETIC.html")}
	d, err = s.Detail(context.Background(), f, r)
	if err != nil || len(d.Websites) != 0 || d.City != "Roma" || d.PostalCode != "" || d.Country != "Italy" {
		t.Errorf("plain detail = %#v, %v", d, err)
	}

	f.pages[r.URL] = Page{Body: []byte("<html><body>nothing</body></html>")}
	if _, err := s.Detail(context.Background(), f, r); err == nil {
		t.Error("layout change must fail")
	}
}
