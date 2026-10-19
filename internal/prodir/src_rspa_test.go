package prodir

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

const rspaTestProfile = rspaProfile + "910001"

// rspaFake serves the search form on GET and, on POST, the results page of the
// posted state option; it fails the request when the viewstate or the Find
// button are missing from the body.
type rspaFake struct {
	fetchGets map[string]Page
	results   map[string][]byte // state option value -> results page
	gets      int
	posted    []string
}

func (f *rspaFake) Get(_ context.Context, u string, _ map[string]string) (Page, error) {
	f.gets++
	p, ok := f.fetchGets[u]
	if !ok {
		return Page{Status: 404, URL: u}, nil
	}
	p.Status, p.URL = 200, u
	return p, nil
}

func (f *rspaFake) Post(_ context.Context, u string, _ map[string]string, body []byte) (Page, error) {
	q, err := url.ParseQuery(string(body))
	if err != nil {
		return Page{}, err
	}
	state := q.Get("ctl01$Sheet0$Input3$DropDown1")
	f.posted = append(f.posted, state)
	if q.Get("__VIEWSTATE") != "synthetic+viewstate/token=" || q.Get("ctl01$Sheet0$SubmitButton") != "Find" || q.Has("ctl01$SearchTerms") {
		return Page{Status: 500, URL: u}, nil
	}
	b, ok := f.results[state]
	if !ok {
		b = f.results[""]
	}
	return Page{Status: 200, URL: u, Body: b}, nil
}

func TestRSPAList(t *testing.T) {
	s := mustSource(t, "rspa")
	f := &rspaFake{
		fetchGets: map[string]Page{rspaFind: {Body: readFixture(t, "rspa_find_SYNTHETIC.html")}},
		results: map[string][]byte{
			"":        readFixture(t, "rspa_results_empty_SYNTHETIC.html"),
			"AL:::0":  readFixture(t, "rspa_results_al_SYNTHETIC.html"),
			"NT:::51": readFixture(t, "rspa_results_nt_SYNTHETIC.html"),
		},
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "910001", URL: rspaTestProfile, Name: "Avery Fixture", City: "Exampleton", Region: "AL", PostalCode: "35117-3815", Country: "United States",
			Meta: map[string][]string{"member_type": {"Certified Professional"}, "tennis_level": {"Professional"}}},
		{ID: "910002", URL: rspaProfile + "910002", Name: "Blake Sample", Business: "Example Tennis Club", City: "Sampleford", Region: "AL", PostalCode: "36526", Country: "United States",
			Meta: map[string][]string{"name_suffix": {"HEHIM"}, "member_type": {"Certified Elite Professional"}, "tennis_level": {"Elite Professional"}, "pickleball_level": {"Professional"}}},
		// the NT option 51 is Australia's Northern Territory
		{ID: "910003", URL: rspaProfile + "910003", Name: "Casey Placeholder", City: "Demoville", Region: "NT", Country: "Australia",
			Meta: map[string][]string{"member_type": {"Certified Instructor"}, "tennis_level": {"Instructor"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// one search per state option, each on a freshly fetched form (1 GET to read the options, then 1 per extra state)
	if !reflect.DeepEqual(f.posted, []string{"AL:::0", "NT:::51", "ON:::57"}) || f.gets != 3 {
		t.Errorf("posted %v after %d GETs", f.posted, f.gets)
	}

	// emit's error (the -limit) stops the walk at once
	f.posted = nil
	err := s.List(context.Background(), f, func(Record) error { return errListLimit })
	if !errors.Is(err, errListLimit) || len(f.posted) != 1 {
		t.Errorf("limit: err=%v posted=%v", err, f.posted)
	}

	// a form that lost its viewstate is a layout error, not an empty directory
	f = &rspaFake{fetchGets: map[string]Page{rspaFind: {Body: []byte(strings.ReplaceAll(string(readFixture(t, "rspa_find_SYNTHETIC.html")), "__VIEWSTATE", "__X"))}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("missing viewstate must fail List")
	}
}

func TestRSPACountry(t *testing.T) {
	for opt, want := range map[string]string{
		"AL:::0": "United States", "WA:::78": "United States", "WA:::80": "Australia", "NT:::51": "Australia",
		"NT:::52": "Canada", "ON:::57": "Canada", "NSW:::45": "Australia", "PR:::62": "United States",
	} {
		if got := rspaCountry(opt); got != want {
			t.Errorf("rspaCountry(%q) = %q, want %q", opt, got, want)
		}
	}
}

func TestRSPADetail(t *testing.T) {
	s := mustSource(t, "rspa")
	f := &fakeFetcher{pages: map[string]Page{
		rspaTestProfile:        {Body: readFixture(t, "rspa_profile_SYNTHETIC.html")},
		rspaProfile + "910003": {Body: readFixture(t, "rspa_profile_minimal_SYNTHETIC.html")},
		rspaProfile + "910004": {Body: []byte("<html><body>nothing here</body></html>")},
	}}
	got, err := s.Detail(context.Background(), f, Record{ID: "910001", URL: rspaTestProfile})
	if err != nil {
		t.Fatal(err)
	}
	want := Record{
		Emails: []string{"avery.fixture@example.com"}, Phones: []string{"(555) 010-0199"}, Websites: []string{"https://www.example.com/coach/"},
		Bio:  "Synthetic coach bio.",
		Meta: map[string][]string{"specialties": {"10 and Under Tennis"}, "level": {"Professional"}, "safe_play": {"APPROVED"}, "certifications": {"Pickleball Certified"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("detail = %#v\nwant     %#v", got, want)
	}

	// empty Website / Email fields add nothing
	got, err = s.Detail(context.Background(), f, Record{ID: "910003", URL: rspaProfile + "910003"})
	if err != nil || got.Emails != nil || got.Websites != nil || got.Phones != nil || !reflect.DeepEqual(got.Meta, map[string][]string{"level": {"Instructor"}}) {
		t.Errorf("minimal detail = %#v, %v", got, err)
	}

	// a gone profile is ErrNotFound; a page without the profile fields is a layout error
	if _, err := s.Detail(context.Background(), f, Record{ID: "1", URL: rspaProfile + "1"}); !errors.Is(err, ErrNotFound) {
		t.Errorf("404: %v", err)
	}
	if _, err := s.Detail(context.Background(), f, Record{ID: "910004", URL: rspaProfile + "910004"}); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("layout change: %v", err)
	}
}
