package prodir

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
)

// collect runs a source's List against f and returns the emitted records.
func collect(t *testing.T, s *Source, f Fetcher) []Record {
	t.Helper()
	var out []Record
	err := s.List(context.Background(), f, func(r Record) error {
		if err := r.check(); err != nil {
			t.Fatalf("%s emitted an invalid record: %v", s.Slug, err)
		}
		out = append(out, r)
		return nil
	})
	if err != nil {
		t.Fatalf("%s List: %v", s.Slug, err)
	}
	return out
}

func mustSource(t *testing.T, slug string) *Source {
	t.Helper()
	s, ok := LookupSource(slug)
	if !ok {
		t.Fatalf("source %s not registered", slug)
	}
	return s
}

func TestIyengarUKList(t *testing.T) {
	s := mustSource(t, "iyengaruk")
	page1 := readFixture(t, "iyengaruk_list_SYNTHETIC.html")
	empty := readFixture(t, "iyengaruk_list_empty_SYNTHETIC.html")
	u := func(n string) string { return iyengarBase + "/teachers-directory/?_paged=" + n }

	f := &fakeFetcher{pages: map[string]Page{u("1"): {Body: page1}, u("2"): {Body: empty}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "alex-fixture-90001", URL: "https://iyengaryoga.org.uk/teacher/alex-fixture-90001/", Name: "Alex Fixture",
			City: "Exampleton", Country: "United Kingdom", Bio: "Synthetic teacher of the fixture method since 1999.", Meta: map[string][]string{"level": {"Level 2"}}},
		{ID: "blair-placeholder-90002", URL: "https://iyengaryoga.org.uk/teacher/blair-placeholder-90002/", Name: "Blair Placeholder",
			City: "Sampleford", Country: "United Kingdom", Meta: map[string][]string{"level": {"Level 1"}}},
		{ID: "cameron-overseas-90003", URL: "https://iyengaryoga.org.uk/teacher/cameron-overseas-90003/", Name: "Cameron Overseas",
			City: "Dublin, Ireland", Country: "Ireland", Meta: map[string][]string{"level": {"Level 3"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 2 { // total_pages=2: pages 1 and 2, then done
		t.Errorf("requests = %v", f.calls)
	}

	// without a pager in the page it walks until a page has no teachers
	noPager := []byte(strings.Replace(string(page1), `"total_pages":2`, `"x":2`, 1))
	f = &fakeFetcher{pages: map[string]Page{u("1"): {Body: noPager}, u("2"): {Body: page1}, u("3"): {Body: empty}}}
	if got := collect(t, s, f); len(got) != 6 || len(f.calls) != 3 {
		t.Errorf("no pager: %d records, requests %v", len(got), f.calls)
	}

	// emit's error (the -limit) stops the walk at once
	f = &fakeFetcher{pages: map[string]Page{u("1"): {Body: page1}, u("2"): {Body: page1}}}
	err := s.List(context.Background(), f, func(Record) error { return errListLimit })
	if !errors.Is(err, errListLimit) || len(f.calls) != 1 {
		t.Errorf("limit: err=%v requests=%v", err, f.calls)
	}

	// an HTTP error fails the stage
	f = &fakeFetcher{pages: map[string]Page{u("1"): {Status: 500}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
}

func TestIyengarUKDetail(t *testing.T) {
	s := mustSource(t, "iyengaruk")
	r := Record{ID: "alex-fixture-90001", URL: iyengarBase + "/teacher/alex-fixture-90001/", City: "Exampleton"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "iyengaruk_profile_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{
		Emails: []string{"alex.fixture@example.org"}, Phones: []string{"01632 960123"},
		Websites: []string{"https://alex-fixture.example.com"},
		Address:  "Flat 4, 1 Example Road", Bio: "Synthetic teacher of the fixture method since 1999.",
		Country: "United Kingdom", Meta: map[string][]string{"level": {"Level 2"}},
	}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}
	// merged: the town of the list card is kept, the detail line becomes the address
	if m := mergeRecord(r, d); m.City != "Exampleton" || m.Address != "Flat 4, 1 Example Road" {
		t.Errorf("merged city=%q address=%q", m.City, m.Address)
	}

	// a Location equal to the list town is not an address
	f.pages[r.URL] = Page{Body: []byte(strings.Replace(string(f.pages[r.URL].Body), "Flat 4, 1 Example Road", "exampleton", 1))}
	if d, _ := s.Detail(context.Background(), f, r); d.Address != "" {
		t.Errorf("address = %q, want empty", d.Address)
	}

	// a location that names another recognised country wins over the UK default,
	// in the detail line and in the list card's town; an unknown last part does not
	body := string(readFixture(t, "iyengaruk_profile_SYNTHETIC.html"))
	for _, c := range []struct{ loc, city, want string }{
		{"Flat 4, 1 Example Road, Ireland", "Exampleton", "Ireland"},
		{"Flat 4, 1 Example Road", "Dublin, Ireland", "Ireland"},
		{"Flat 4, 1 Example Road, Exampleshire", "Exampleton", "United Kingdom"},
	} {
		f.pages[r.URL] = Page{Body: []byte(strings.Replace(body, "Flat 4, 1 Example Road", c.loc, 1))}
		d, err := s.Detail(context.Background(), f, Record{ID: r.ID, URL: r.URL, City: c.city})
		if err != nil {
			t.Fatal(err)
		}
		if d.Country != c.want {
			t.Errorf("location %q city %q: country = %q, want %q", c.loc, c.city, d.Country, c.want)
		}
	}
	f.pages[r.URL] = Page{Body: []byte(body)}

	// the default resolves to GB, the exception to its own code
	res := newCountryResolver()
	if l := recordListing(s, Record{ID: "a", Country: "United Kingdom"}, res); l.CountryCode != "GB" {
		t.Errorf("United Kingdom -> %q", l.CountryCode)
	}
	if l := recordListing(s, Record{ID: "b", Country: "Ireland"}, res); l.CountryCode != "IE" {
		t.Errorf("Ireland -> %q", l.CountryCode)
	}

	// gone and layout-changed pages
	if _, err := s.Detail(context.Background(), &fakeFetcher{}, r); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: %v, want ErrNotFound", err)
	}
	f.pages[r.URL] = Page{Body: []byte("<html><body>new layout</body></html>")}
	if _, err := s.Detail(context.Background(), f, r); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("layout change: %v, want a plain error", err)
	}
}

func TestSharathList(t *testing.T) {
	s := mustSource(t, "sharath")
	f := &fakeFetcher{pages: map[string]Page{sharathListURL: {Body: readFixture(t, "sharath_list_SYNTHETIC.html")}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "9001", URL: "https://sharathyogacentre.com/authorised-teachers-directory/casey-fixture/", Name: "Casey Fixture",
			Business: "Fixture Shala", City: "Exampleton", Region: "Exampleshire", Country: "Japan",
			Meta: map[string][]string{"authorisation_level": {"Certified"}}},
		{ID: "9002", URL: "https://sharathyogacentre.com/authorised-teachers-directory/devon-placeholder/", Name: "Devon Placeholder",
			Country: "USA", Meta: map[string][]string{"authorisation_level": {"Level 1"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 1 {
		t.Errorf("the whole directory is one request, got %v", f.calls)
	}
}

func TestSharathDetail(t *testing.T) {
	s := mustSource(t, "sharath")
	r := Record{ID: "9001", URL: "https://sharathyogacentre.com/authorised-teachers-directory/casey-fixture/"}
	f := &fakeFetcher{pages: map[string]Page{r.URL: {Body: readFixture(t, "sharath_profile_SYNTHETIC.html")}}}
	d, err := s.Detail(context.Background(), f, r)
	if err != nil {
		t.Fatal(err)
	}
	want := Record{
		Business: "Fixture Shala", City: "Exampleton", Country: "Japan",
		Emails: []string{"casey.fixture@example.org"}, Phones: []string{"+81 555 010 0123"},
		Websites: []string{"https://fixture-shala.example.com"},
		Address:  "2F, Fixture Building, 1-2-3 Example-cho", PostalCode: "100-0001",
		Meta: map[string][]string{"authorisation_level": {"Certified"}},
	}
	if !reflect.DeepEqual(d, want) {
		t.Errorf("detail = %#v\nwant     %#v", d, want)
	}
	// the centre's own footer contact (centre@example.net, +91 ...) is not the teacher's
	for _, e := range d.Emails {
		if strings.HasPrefix(e, "centre@") {
			t.Errorf("took the footer email %q", e)
		}
	}

	if _, err := s.Detail(context.Background(), &fakeFetcher{}, r); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: %v, want ErrNotFound", err)
	}
	f.pages[r.URL] = Page{Body: []byte("<html><body>new layout</body></html>")}
	if _, err := s.Detail(context.Background(), f, r); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("layout change: %v, want a plain error", err)
	}
}

func TestCrossFitAffiliatesList(t *testing.T) {
	s := mustSource(t, "crossfit-affiliates")
	if s.Detail != nil {
		t.Error("crossfit-affiliates has no detail stage")
	}
	u := func(n string) string { return crossfitURL + "?page=" + n }
	f := &fakeFetcher{pages: map[string]Page{
		u("1"): {Body: readFixture(t, "crossfit-affiliates_page1_SYNTHETIC.json")},
		u("2"): {Body: readFixture(t, "crossfit-affiliates_page2_SYNTHETIC.json")},
		u("3"): {Body: readFixture(t, "crossfit-affiliates_empty_SYNTHETIC.json")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "90001", Name: "CrossFit Fixture", Address: "1 Example Circle", City: "Exampleville", PostalCode: "00001",
			Region: "Exampleland", Country: "United States", Websites: []string{"https://crossfit-fixture.example.com/home/"},
			Meta: map[string][]string{"status": {"0"}, "kids": {"1"}, "established_date": {"2004-09-13"}}},
		{ID: "90004", Name: "CrossFit No Site Fixture", Address: "4 Sample Way", City: "Placeholder",
			Region: "ON", Country: "CA", Meta: map[string][]string{"status": {"2"}, "kids": {"0"}}},
		// no bad_standing field at all, hidden from the map: still a live box
		{ID: "90005", Name: "CrossFit No Standing Field Fixture", Address: "5 Sample Way", City: "Noflag", PostalCode: "00005",
			Region: "Exampleland", Country: "United States",
			Meta: map[string][]string{"status": {"0"}, "kids": {"0"}, "established_date": {"2015-05-05"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// 90002 is in bad standing, 90003 inactive, 90001 repeats on page 2
	if len(f.calls) != 3 {
		t.Errorf("requests = %v", f.calls)
	}

	// the limit stops after page 1
	f.calls = nil
	err := s.List(context.Background(), f, func(Record) error { return errListLimit })
	if !errors.Is(err, errListLimit) || len(f.calls) != 1 {
		t.Errorf("limit: err=%v requests=%v", err, f.calls)
	}

	// a body that is not JSON fails the stage
	bad := &fakeFetcher{pages: map[string]Page{u("1"): {Body: []byte("<html>blocked</html>")}}}
	if err := s.List(context.Background(), bad, func(Record) error { return nil }); err == nil {
		t.Error("non-JSON body must fail List")
	}
}

// The three proof sources end to end through the generic mapping.
func TestProofSourcesListings(t *testing.T) {
	res := newCountryResolver()
	cf := mustSource(t, "crossfit-affiliates")
	var recs []Record
	f := &fakeFetcher{pages: map[string]Page{
		crossfitURL + "?page=1": {Body: readFixture(t, "crossfit-affiliates_page1_SYNTHETIC.json")},
		crossfitURL + "?page=2": {Body: readFixture(t, "crossfit-affiliates_empty_SYNTHETIC.json")},
	}}
	recs = collect(t, cf, f)
	l := recordListing(cf, recs[0], res)
	if l.Domain != "90001.crossfit-affiliates.crossfit.com" || l.Niche != "fitness" || l.Category != "crossfit_trainer" ||
		!l.OwnWebsite || l.Website != "https://crossfit-fixture.example.com/home/" || l.CountryCode != "US" ||
		l.Location != "Exampleville, Exampleland, United States" || l.URL != "https://crossfit.com" ||
		!reflect.DeepEqual(l.Meta["listing_type"], []string{"gym"}) {
		t.Errorf("crossfit listing = %+v", l)
	}

	sh := mustSource(t, "sharath")
	r := Record{ID: "9001", Name: "Casey Fixture", Business: "Fixture Shala", Country: "USA", Meta: map[string][]string{"authorisation_level": {"Certified"}}}
	l = recordListing(sh, r, res)
	if l.Domain != "9001.sharath.sharathyogacentre.com" || l.Niche != "yoga" || l.CountryCode != "US" ||
		!reflect.DeepEqual(l.Meta["business_name"], []string{"Fixture Shala"}) {
		t.Errorf("sharath listing = %+v", l)
	}
}

func TestCrossFitLive(t *testing.T) {
	for _, c := range []struct {
		a    crossfitAffiliate
		want bool
	}{
		{crossfitAffiliate{Active: "1", BadStanding: "0"}, true},
		{crossfitAffiliate{Active: "1"}, true}, // no bad_standing field
		{crossfitAffiliate{Active: "1", BadStanding: "1"}, false},
		{crossfitAffiliate{Active: "0", BadStanding: "0"}, false},
		{crossfitAffiliate{BadStanding: "0"}, false}, // no active field
	} {
		if got := c.a.live(); got != c.want {
			t.Errorf("%+v live = %v, want %v", c.a, got, c.want)
		}
	}
}
