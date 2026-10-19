package prodir

import (
	"context"
	"reflect"
	"strings"
	"testing"
)

func TestHot90List(t *testing.T) {
	s := mustSource(t, "hot90")
	const q = hot90Base + "/api/v3/queryCollection"
	f := &fakeFetcher{pages: map[string]Page{q: {Body: readFixture(t, "hot90_query_SYNTHETIC.json")}}}
	got := collect(t, s, f)
	// rows without a name, marked Closed or not alive are skipped
	want := []Record{
		{ID: "aaaaaaaa-0000-4000-8000-000000000001", URL: hot90Base + "/aaaaaaaa000040008000000000000001", Name: "Fixture Hot Studio",
			Address: "1 Example Way", City: "Fixtureville", Region: "CA", PostalCode: "95000", Country: "United States",
			Websites: []string{"https://fixture-hot.example.com/"}, // tracking query dropped
			Social:   map[string]string{"instagram": "https://www.instagram.com/fixture_hot/"},
			Meta:     map[string][]string{"region_group": {"Bay Area"}, "status": {"Verified"}}},
		{ID: "aaaaaaaa-0000-4000-8000-000000000002", URL: hot90Base + "/aaaaaaaa000040008000000000000002", Name: "Sample Heat",
			City: "Sampleton", Country: "United States", Websites: []string{"https://sampleheat.example.org"},
			Meta: map[string][]string{"region_group": {"Southern California"}, "status": {"Verified"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	if len(f.calls) != 1 {
		t.Errorf("requests = %v", f.calls)
	}

	// more rows than the limit asked for must fail, not truncate
	f = &fakeFetcher{pages: map[string]Page{q: {Body: readFixture(t, "hot90_query_hasmore_SYNTHETIC.json")}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil || !strings.Contains(err.Error(), "more than") {
		t.Errorf("hasMore: err = %v", err)
	}
	// an HTTP error fails the stage
	f = &fakeFetcher{pages: map[string]Page{q: {Status: 500}}}
	if err := s.List(context.Background(), f, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 500 must fail List")
	}
	// the record maps to a listing in its own domain
	if d := sourceDomain(s, want[0].ID); d != "aaaaaaaa-0000-4000-8000-000000000001.hot90.hot90finder.notion.site" {
		t.Errorf("domain %q", d)
	}
}
