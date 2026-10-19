package prodir

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

// bcrpaFetcher answers the JSON POST by the page number in the body.
type bcrpaFetcher struct {
	pages map[string][]byte // `"page":N,` marker -> body
	posts []string
}

func (f *bcrpaFetcher) Get(context.Context, string, map[string]string) (Page, error) {
	return Page{Status: 404}, nil
}

func (f *bcrpaFetcher) Post(_ context.Context, u string, hdr map[string]string, body []byte) (Page, error) {
	f.posts = append(f.posts, string(body))
	if hdr["Content-Type"] != "application/json" {
		return Page{Status: 400, URL: u}, nil
	}
	for marker, b := range f.pages {
		if strings.Contains(string(body), marker) {
			return Page{Status: 200, URL: u, Body: b}, nil
		}
	}
	return Page{Status: 404, URL: u}, nil
}

func TestBCRPAList(t *testing.T) {
	s := mustSource(t, "bcrpa")
	f := &bcrpaFetcher{pages: map[string][]byte{
		`"page":0,`: readFixture(t, "bcrpa_page0_SYNTHETIC.json"),
		`"page":1,`: readFixture(t, "bcrpa_page1_SYNTHETIC.json"),
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "80001", Name: "Alex Fixture", Business: "Fixture Fitness", Address: "1 Example St Unit 2", City: "Exampleton",
			Region: "British Columbia", PostalCode: "V0X0X0", Country: "CANADA", Bio: "Synthetic bio.",
			Emails: []string{"alex@example.com"}, Phones: []string{"(250) 555-0101"}, Websites: []string{"www.alex-fixture.example.com"},
			Social: map[string]string{"facebook": "https://www.facebook.com/alexfixture"},
			Meta: map[string][]string{"member_type": {"Fitness Leader"}, "title": {"Owner"},
				"credential":  {"Personal Trainer", "Weight Training"},
				"specialties": {"Fitness Theory", "Group Fitness", "Weight Training"}}},
		{ID: "80002", Name: "Blair Placeholder", Country: "Canada", Emails: []string{"blair@example.org"},
			Social: map[string]string{}, Meta: map[string][]string{"member_type": {"Fitness Student"}}},
		{ID: "80004", Name: "Casey Overseas", City: "Testville", Region: "WA", Country: "United States",
			Emails: []string{"casey@example.com"}, Social: map[string]string{},
			Meta: map[string][]string{"member_type": {"Fitness Leader"}, "specialties": {"Yoga", "Spin"}}},
	}
	// the IsPrivate row, the repeated id and the nameless row are skipped
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.posts) != 2 || !strings.Contains(f.posts[1], `"limit":500`) { // NextPage=-1 ends the walk
		t.Errorf("posts = %v", f.posts)
	}
}

func TestBCRPAListing(t *testing.T) {
	s := mustSource(t, "bcrpa")
	l := recordListing(s, Record{ID: "80001", Name: "Alex Fixture", Country: "CANADA", Websites: []string{"www.alex-fixture.example.com"}}, newCountryResolver())
	if l.CountryCode != "CA" || l.Website == "" || strings.Contains(l.Website, "thefitnessregistry") {
		t.Errorf("listing = %+v", l)
	}
}
