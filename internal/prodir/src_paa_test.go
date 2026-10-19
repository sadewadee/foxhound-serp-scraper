package prodir

import (
	"context"
	"errors"
	"reflect"
	"testing"
)

// paaFetcher records the headers of the API calls on top of a fakeFetcher.
type paaFetcher struct {
	fakeFetcher
	hdrs []map[string]string
}

func (p *paaFetcher) Get(ctx context.Context, url string, hdr map[string]string) (Page, error) {
	p.hdrs = append(p.hdrs, hdr)
	return p.fakeFetcher.Get(ctx, url, hdr)
}

func TestPAAList(t *testing.T) {
	s := mustSource(t, "paa")
	f := &paaFetcher{fakeFetcher: fakeFetcher{pages: map[string]Page{
		paaBase + "/MemberSearch": {Body: readFixture(t, "paa_membersearch_SYNTHETIC.html")},
		paaIQAURL(paaMembersQ, 0): {Body: readFixture(t, "paa_members_p1_SYNTHETIC.json")},
		paaIQAURL(paaMembersQ, 2): {Body: readFixture(t, "paa_members_p2_SYNTHETIC.json")},
		paaIQAURL(paaStudiosQ, 0): {Body: readFixture(t, "paa_studios_SYNTHETIC.json")},
	}}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "m-70001", Name: "Alex Fixture", Business: "Example Pilates Studio", Address: "1 Example St Exampleville NSW 2000",
			City: "Exampleville", PostalCode: "2000", Country: "Australia",
			Emails: []string{"alex.fixture@example.org"}, Phones: []string{"0455 555 101", "02 5550 0102"},
			Websites: []string{"http://www.example-pilates.example.com"},
			Meta: map[string][]string{"practitioner_level": {"L2"}, "group_instructor_category": {"GPMat", "GPRef"},
				"member_type": {"PAA Practitioner - Level 2"}}},
		// no MemberName: first + last name; no country: the national default; code-only member type
		{ID: "m-70002", Name: "Blair Placeholder", Country: "Australia", Emails: []string{"blair.placeholder@example.com"},
			Meta: map[string][]string{"member_type": {"PAA Studio Instruct."}}},
		// a facebook page in the website field stays a website value here (the loader turns it into social)
		{ID: "m-70003", Name: "Cameron Sample", Business: "Sample Studio", Address: "5 Sample Rd Sampleton VIC 3000",
			City: "Sampleton", PostalCode: "3000", Country: "Australia", Emails: []string{"cameron.sample@example.org"},
			Phones: []string{"0455 555 103"}, Websites: []string{"https://www.facebook.com/samplestudio"},
			Meta: map[string][]string{"member_type": {"PAA Practitioner - PT"}}},
		// studio rows: "s-" ids (never collide with members), the company is the name
		{ID: "s-80001", Name: "Example Studio Collective", Address: "9 Studio Way Exampleville NSW 2000", City: "Exampleville",
			PostalCode: "2000", Country: "Australia", Emails: []string{"studio@example.com"},
			Phones:   []string{"0455 555 104", "02 5550 0105"},
			Websites: []string{"https://example-collective.example.com"},
			Meta:     map[string][]string{"member_type": {"STDIO"}}},
		{ID: "s-70001", Name: "Same Id As Member", Country: "Australia", Emails: []string{"x@example.com"},
			Meta: map[string][]string{"member_type": {"STDIO"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// page GET, members page 1 + NextOffset page, studios
	if len(f.calls) != 4 {
		t.Errorf("requests = %v", f.calls)
	}
	for _, h := range f.hdrs[1:] {
		if h["RequestVerificationToken"] != "synthetic-token-0123456789" || h["Accept"] != "application/json" {
			t.Errorf("api headers = %v", h)
		}
	}

	// -limit
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); !errors.Is(err, errListLimit) {
		t.Errorf("limit: %v", err)
	}
	// a page without the token is a layout change
	f = &paaFetcher{fakeFetcher: fakeFetcher{pages: map[string]Page{paaBase + "/MemberSearch": {Body: []byte("<html></html>")}}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("missing token must fail List")
	}
	// an API error fails the stage
	f = &paaFetcher{fakeFetcher: fakeFetcher{pages: map[string]Page{
		paaBase + "/MemberSearch": {Body: readFixture(t, "paa_membersearch_SYNTHETIC.html")},
	}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("iqa 404 must fail List")
	}
}

func TestPAAIQAURL(t *testing.T) {
	got := paaIQAURL("$/PseudoCode/Search-Buddy/Search Buddy - Online Directory Members", 0)
	want := "https://member.pilates.org.au/api/iqa?Limit=500&QueryName=%24%2FPseudoCode%2FSearch-Buddy%2FSearch%20Buddy%20-%20Online%20Directory%20Members"
	if got != want {
		t.Errorf("url = %s", got)
	}
}
