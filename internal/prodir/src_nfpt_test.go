package prodir

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"
)

// nfptFetcher answers the POST helpers by form body (fakeFetcher ignores it).
type nfptFetcher struct {
	byBody map[string][]byte
	empty  []byte
	posts  int
}

func (f *nfptFetcher) Get(context.Context, string, map[string]string) (Page, error) {
	return Page{Status: 404}, nil
}

func (f *nfptFetcher) Post(_ context.Context, u string, hdr map[string]string, body []byte) (Page, error) {
	f.posts++
	if hdr["Content-Type"] != "application/x-www-form-urlencoded" {
		return Page{Status: 400, URL: u}, nil
	}
	b, ok := f.byBody[u+"|"+string(body)]
	if !ok {
		b = f.empty
	}
	return Page{Status: 200, URL: u, Body: b}, nil
}

func nfptKey(helper string, kv ...string) string {
	v := url.Values{}
	for i := 0; i < len(kv); i += 2 {
		v.Set(kv[i], kv[i+1])
	}
	return nfptHelpers + helper + "|" + v.Encode()
}

func TestNFPTGridBounded(t *testing.T) {
	n := len(nfptGrid())
	if n < 3000 || n > 7000 {
		t.Errorf("grid has %d points, want a few thousand", n)
	}
	t.Logf("grid points: %d", n)
}

func TestNFPTListAndDetail(t *testing.T) {
	s := mustSource(t, "nfpt")
	f := &nfptFetcher{
		empty: []byte(`{"success":true,"message":"","data":[]}`),
		byBody: map[string][]byte{
			nfptKey("GetTrainers.php", "latitude", "40.71", "longitude", "-74.01"):  readFixture(t, "nfpt_list_SYNTHETIC.json"),
			nfptKey("GetTrainers.php", "latitude", "-33.87", "longitude", "151.21"): readFixture(t, "nfpt_list_overlap_SYNTHETIC.json"),
			nfptKey("GetProfile.php", "profile_address", "90001"):                   readFixture(t, "nfpt_profile_SYNTHETIC.json"),
			nfptKey("GetProfile.php", "profile_address", "90003"):                   readFixture(t, "nfpt_profile_au_SYNTHETIC.json"),
			nfptKey("GetProfile.php", "profile_address", "90002"):                   readFixture(t, "nfpt_profile_unavailable_SYNTHETIC.json"),
		},
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "90001", URL: "https://nfpt.com/trainer-profile/?id=90001", Name: "Alex Fixture", City: "Exampleton", Region: "NY", Country: "USA"},
		{ID: "90002", URL: "https://nfpt.com/trainer-profile/?id=90002", Name: "Blair Placeholder", City: "Sampleford", Region: "New York", Country: "United States"},
		{ID: "90003", URL: "https://nfpt.com/trainer-profile/?id=90003", Name: "Casey Overseas", City: "Testville", Region: "NSW", Country: "Australia"},
	}
	if !reflect.DeepEqual(got, want) { // 90002 is returned twice: deduped
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if f.posts != len(nfptGrid()) {
		t.Errorf("posts = %d, want one per grid point (%d)", f.posts, len(nfptGrid()))
	}

	d, err := s.Detail(context.Background(), f, got[0])
	if err != nil {
		t.Fatal(err)
	}
	wantD := Record{Bio: "Synthetic trainer bio. Reach me at alex@example.com.", Websites: []string{"https://alex-fixture.example.com"},
		Phones: []string{"+15555550101"}, Meta: map[string][]string{"years_certified": {"3"}}}
	if !reflect.DeepEqual(d, wantD) {
		t.Errorf("detail = %#v\nwant     %#v", d, wantD)
	}

	// an Australian 10-digit number is not +1; education becomes a meta list; null about/url add nothing
	d, err = s.Detail(context.Background(), f, got[2])
	if err != nil {
		t.Fatal(err)
	}
	wantD = Record{Phones: []string{"4155550102"}, Meta: map[string][]string{
		"years_certified": {"27"}, "specialist_credentials": {"Fitness Nutrition Specialist", "Cancer Recovery Specialist"}}}
	if !reflect.DeepEqual(d, wantD) {
		t.Errorf("detail = %#v\nwant     %#v", d, wantD)
	}

	// success=false (no longer certified) is a Missing profile
	if _, err := s.Detail(context.Background(), f, got[1]); !errors.Is(err, ErrNotFound) {
		t.Errorf("unavailable profile: err = %v, want ErrNotFound", err)
	}
}

func TestNFPTListing(t *testing.T) {
	s := mustSource(t, "nfpt")
	r := Record{ID: "90001", Name: "Alex Fixture", City: "Exampleton", Country: "USA", Phones: []string{"+15555550101"}}
	l := recordListing(s, r, newCountryResolver())
	if l.CountryCode != "US" || l.Category != "fitness_trainer" || l.Niche != "fitness" {
		t.Errorf("listing = %+v", l)
	}
}
