package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestBhsList(t *testing.T) {
	s := mustSource(t, "bhs")
	u := func(n string) string { return bhsList + "?page=" + n }
	f := &fakeFetcher{pages: map[string]Page{
		u("1"): {Body: readFixture(t, "bhs_list_SYNTHETIC.html")},
		u("2"): {Body: readFixture(t, "bhs_list_empty_SYNTHETIC.html")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "fixture-riding-club", URL: bhsList + "fixture-riding-club", Name: "Fixture Riding Club Ltd",
			City: "Exampleton", Region: "Scotland", Country: "United Kingdom", Phones: []string{"01632 960001"},
			Meta: map[string][]string{"services": {"Riding school", "Livery"}}},
		{ID: "placeholder-stables", URL: bhsList + "placeholder-stables", Name: "Placeholder Stables",
			City: "Sampleford", Region: "England", Country: "United Kingdom", Phones: []string{"01632 960002"}, Meta: map[string][]string{}},
		{ID: "overseas-stud", URL: bhsList + "overseas-stud", Name: "Overseas Stud",
			City: "Dubai", Region: "Dubai", Phones: []string{"(+971) 55 5550100"},
			Meta: map[string][]string{"services": {"Livery"}}},
	}
	if !reflect.DeepEqual(got, want) { // duplicate slug and nameless card are dropped; a foreign place has no country yet
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 {
		t.Errorf("requests = %v", f.calls)
	}
	if err := s.List(context.Background(), &fakeFetcher{pages: map[string]Page{u("1"): {Status: 500}}}, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
}

func TestBhsDetail(t *testing.T) {
	s := mustSource(t, "bhs")
	r := Record{ID: "fixture-riding-club", URL: bhsList + "fixture-riding-club", Region: "Scotland"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "bhs_detail_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{
		Phones: []string{"01632 960001"}, Emails: []string{"info@example.org"},
		Websites: []string{"https://fixture-riding.example.com/"},
		Address:  "1 Example Rd, Exampleton AB14 0PN, UK", PostalCode: "AB14 0PN", Country: "United Kingdom",
		Bio: "Synthetic riding club. Second paragraph.",
	}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}
	if l := recordListing(s, mergeRecord(r, d), newCountryResolver()); l.CountryCode != "GB" || l.Category != "sports_equestrian" {
		t.Errorf("listing = %+v", l)
	}

	// a foreign centre: the address suffix names the country, phone entity decoded, no email / website
	o := Record{ID: "overseas-stud", URL: bhsList + "overseas-stud", Region: "Dubai"}
	f.pages[o.URL] = Page{Body: readFixture(t, "bhs_detail_overseas_SYNTHETIC.html")}
	d, err = s.Detail(context.Background(), f, o)
	if err != nil || d.Country != "United Arab Emirates" || len(d.Emails) != 0 || len(d.Websites) != 0 ||
		!reflect.DeepEqual(d.Phones, []string{"(+971) 55 5550100"}) || d.PostalCode != "" {
		t.Errorf("overseas detail = %#v, %v", d, err)
	}

	f.pages[r.URL] = Page{Body: []byte("<html><body>nothing</body></html>")}
	if _, err := s.Detail(context.Background(), f, r); err == nil {
		t.Error("layout change must fail")
	}
}
