package prodir

import (
	"reflect"
	"testing"
)

func TestEvolationList(t *testing.T) {
	s := mustSource(t, "evolation")
	f := &fakeFetcher{pages: map[string]Page{evolationURL: {Body: readFixture(t, "evolation_studios_SYNTHETIC.json")}}}
	got := collect(t, s, f)
	us, intl := map[string][]string{"section": {"united_states"}}, map[string][]string{"section": {"international"}}
	const u = "https://evolationyoga.com/studios"
	want := []Record{
		{ID: "fixture-hot-yoga-example-com", URL: u, Name: "fixture-hot-yoga.example.com",
			Websites: []string{"http://www.fixture-hot-yoga.example.com"}, Meta: us},
		{ID: "sampleflow-example-org", URL: u, Name: "sampleflow.example.org",
			Websites: []string{"https://sampleflow.example.org/studio"}, Meta: us},
		// social-only card; the bare instagram.com link and the mailto are dropped
		{ID: "instagram-fixture-studio-ig", URL: u, Name: "fixture_studio_ig",
			Social: map[string]string{"instagram": "https://www.instagram.com/fixture_studio_ig/"}, Meta: us},
		{ID: "placeholder-yoga-example-de", URL: u, Name: "placeholder-yoga.example.de",
			Websites: []string{"http://placeholder-yoga.example.de"}, Meta: intl},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 1 {
		t.Errorf("requests = %v", f.calls)
	}
	if s.Detail != nil || s.Category != "hot_yoga_teacher" || s.ListingType != "studio" {
		t.Error("evolation: no detail, hot_yoga_teacher, studio")
	}
}
