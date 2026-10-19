package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestPtrList(t *testing.T) {
	s := mustSource(t, "ptr")
	u := func(n string) string { return ptrURL + "?pageSize=100&page=" + n }
	f := &fakeFetcher{pages: map[string]Page{
		u("1"): {Body: readFixture(t, "ptr_page1_SYNTHETIC.json")},
		u("2"): {Body: readFixture(t, "ptr_page2_SYNTHETIC.json")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "70001", Name: "Alex Fixture", Business: "Fixture Tennis Club", Address: "1 Example Rd Suite 5",
			City: "Exampleton", Region: "TX", PostalCode: "75000", Country: "US",
			Emails: []string{"alex.fixture@example.com"}, Phones: []string{"555-0101"},
			Bio: "Synthetic coach, write me at coach@example.org.",
			Meta: map[string][]string{
				"credential": {"Professional", "Pickle"}, "certification_category": {"Tennis", "Pickleball"},
				"level": {"Coach"}, "member_number": {"M70001"}, "lesson_price_hourly": {"60"},
				"offers": {"private lessons", "clinics"}}},
		{ID: "70002", Name: "Blair Placeholder", City: "Sampleford", Country: "IT",
			Emails: []string{"blair@example.org"},
			Meta:   map[string][]string{"member_number": {"M70002"}, "offers": {"private lessons", "clinics"}}},
		{ID: "70005", Name: "Cameron Overseas", Business: "Example Racquet Club", City: "Paris", Country: "FR",
			Emails: []string{"cam@example.com"}, Phones: []string{"555-0102"},
			Meta: map[string][]string{"member_number": {"M70005"}, "offers": {"private lessons", "clinics"}}},
	}
	if !reflect.DeepEqual(got, want) { // inactive, duplicate id and blank name are dropped; a non-numeric price is ignored
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 { // total=150 at pageSize 100: two pages
		t.Errorf("requests = %v", f.calls)
	}

	l := recordListing(s, got[0], newCountryResolver())
	if l.CountryCode != "US" || l.Category != "sports_tennis" || len(l.Emails) != 2 || len(l.Phones) != 1 {
		t.Errorf("listing = %+v", l) // the bio email joins the structured one
	}

	// limit stops the walk after the first page
	f = &fakeFetcher{pages: map[string]Page{u("1"): {Body: readFixture(t, "ptr_page1_SYNTHETIC.json")}}}
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); err != errListLimit || len(f.calls) != 1 {
		t.Errorf("limit: err=%v requests=%v", err, f.calls)
	}
	if err := s.List(context.Background(), &fakeFetcher{pages: map[string]Page{u("1"): {Status: 500}}}, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
	if err := s.List(context.Background(), &fakeFetcher{pages: map[string]Page{u("1"): {Body: []byte("<html>")}}}, func(Record) error { return nil }); err == nil {
		t.Error("a non-JSON body must fail List")
	}
}

// the API sends some string fields as numbers or null (a certification level, a zip)
func TestPtrLooseTypes(t *testing.T) {
	s := mustSource(t, "ptr")
	body := `{"items":[{"id":7,"firstName":"Alex","lastName":"Fixture","email":"a@example.com","zip":12345,"state":null,
		"certifications":[{"certificationName":"Pro","category":null,"level":2}]}],"total":1,"pageSize":100}`
	f := &fakeFetcher{pages: map[string]Page{ptrURL + "?pageSize=100&page=1": {Body: []byte(body)}}}
	got := collect(t, s, f)
	if len(got) != 1 || got[0].ID != "7" || got[0].PostalCode != "12345" || got[0].Region != "" ||
		!reflect.DeepEqual(got[0].Meta["level"], []string{"2"}) {
		t.Errorf("records = %#v", got)
	}
}
