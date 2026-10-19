package prodir

import (
	"context"
	"reflect"
	"testing"
)

func TestCrossfitTrainersList(t *testing.T) {
	s := mustSource(t, "crossfit-trainers")
	u := func(n string) string { return crossfitTrainersAPI + "?page=" + n + "&per_page=10" }
	f := &fakeFetcher{pages: map[string]Page{
		u("1"): {Body: readFixture(t, "crossfit-trainers_page1_SYNTHETIC.json")},
		u("2"): {Body: readFixture(t, "crossfit-trainers_page2_SYNTHETIC.json")},
		u("3"): {Body: []byte("[]")},
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "70001", Name: "Alex Fixture", City: "Exampleton", Region: "CA", PostalCode: "90001", Country: "United States",
			Bio: "Fixture coach\nSynthetic coach.", Emails: []string{"alex@example.com", "alex2@example.org"},
			Meta: map[string][]string{"specialties": {"Scaling"}, "level": {"Certified CrossFit Level 4 Coach (CF-L4)"}, "open_to_work": {"yes"}}},
		// is_contactable=false: the listing stays, its emails are dropped
		{ID: "70002", Name: "Blair Placeholder", City: "Sampleford", Country: "United Kingdom",
			Meta: map[string][]string{"level": {"L1"}}},
		{ID: "70004", Name: "Casey Overseas", Country: "IT", Emails: []string{"casey@example.com"},
			Meta: map[string][]string{"level": {"OL1"}}},
	}
	// the is_visible=false trainer is skipped and user_id 70002 is not emitted twice
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 3 { // an empty array ends the walk
		t.Errorf("requests = %v", f.calls)
	}
}

func TestCrossfitTrainersHeaders(t *testing.T) {
	s := mustSource(t, "crossfit-trainers")
	var seen map[string]string
	f := &hdrFetcher{on: func(h map[string]string) { seen = h }}
	collect(t, s, f)
	if seen["Origin"] != "https://www.crossfit.com" || seen["Referer"] != "https://www.crossfit.com/" {
		t.Errorf("headers = %v", seen)
	}
}

// hdrFetcher records the headers of a GET and answers with an empty array.
type hdrFetcher struct {
	fakeFetcher
	on func(map[string]string)
}

func (h *hdrFetcher) Get(_ context.Context, u string, hdr map[string]string) (Page, error) {
	h.on(hdr)
	return Page{Status: 200, URL: u, Body: []byte("[]")}, nil
}
