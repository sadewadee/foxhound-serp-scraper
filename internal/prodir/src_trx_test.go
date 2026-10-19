package prodir

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

// trxFetcher serves the API by query string; any other filter is an empty page.
type trxFetcher struct {
	byQuery map[string][]byte
	calls   []string
}

func (f *trxFetcher) Get(_ context.Context, u string, _ map[string]string) (Page, error) {
	f.calls = append(f.calls, u)
	b, ok := f.byQuery[strings.TrimPrefix(u, trxAPI+"?type=trainers&size=1000&")]
	if !ok {
		b = []byte(`{"items":[]}`)
	}
	return Page{Status: 200, URL: u, Body: b}, nil
}

func (f *trxFetcher) Post(context.Context, string, map[string]string, []byte) (Page, error) {
	return Page{Status: 404}, nil
}

func TestTRXList(t *testing.T) {
	s := mustSource(t, "trx")
	f := &trxFetcher{byQuery: map[string][]byte{
		"page=1":                readFixture(t, "trx_items_SYNTHETIC.json"),
		"page=1&countryCode=GB": readFixture(t, "trx_country_SYNTHETIC.json"),
		"page=1&countryCode=US": readFixture(t, "trx_items_SYNTHETIC.json"),
	}}
	got := collect(t, s, f)
	want := []Record{
		{ID: "00000000-0000-4000-8000-000000000001", Name: "Alex Fixture", City: "Exampleton", Address: "1 Example St, Exampleton",
			PostalCode: "90001", Country: "US", Bio: "Synthetic suspension trainer. Contact alex@example.com.",
			Websites: []string{"https://alex-fixture.example.com"}, Phones: []string{"+1 555 555 0111"},
			Social: map[string]string{"instagram": "https://www.instagram.com/alex.fixture", "facebook": "https://www.facebook.com/alex.fixture"},
			Meta: map[string][]string{
				"credential": {"TRX Suspension Training Qualified", "TRX TEAM"},
				"services":   {"Functional Training", "Suspension Training"},
				"languages":  {"English"}, "locations": {"Exampleton", "Sampleford"}, "rank": {"Black & Yellow"}}},
		{ID: "00000000-0000-4000-8000-000000000002", Name: "Blair Placeholder",
			Social: map[string]string{"tiktok": "https://www.tiktok.com/@blair_p"},
			Meta:   map[string][]string{"credential": {"TRX Suspension Training Qualified"}, "rank": {"Gray"}}},
		{ID: "00000000-0000-4000-8000-000000000004", Name: "Casey Overseas", City: "London", PostalCode: "SW1A", Country: "GB",
			Social: map[string]string{},
			Meta:   map[string][]string{"credential": {"TRX Group Training Qualified"}, "locations": {"London"}}},
	}
	if !reflect.DeepEqual(got, want) { // rows without id / name skipped, repeats across partitions deduped
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// unfiltered pass, then the countries, then the 100 postal prefixes
	if n := len(trxFilters()); n < 100+100 || !strings.Contains(strings.Join(f.calls, " "), "postalCode=99") {
		t.Errorf("filters = %d, calls = %d", n, len(f.calls))
	}
}

func TestTRXPagination(t *testing.T) {
	s := mustSource(t, "trx")
	full := func(from int) []byte {
		var items []map[string]any
		for i := 0; i < trxSize; i++ {
			items = append(items, map[string]any{"id": fmt.Sprintf("id-%d", from+i), "name": "Synthetic Person"})
		}
		b, _ := json.Marshal(map[string]any{"items": items})
		return b
	}
	f := &trxFetcher{byQuery: map[string][]byte{
		"page=1": full(0), "page=2": full(trxSize), "page=3": []byte(`{"items":[{"id":"last","name":"Last Person"}]}`),
	}}
	got := collect(t, s, f)
	if len(got) != 2*trxSize+1 {
		t.Errorf("records = %d, want %d", len(got), 2*trxSize+1)
	}
	// a full page asks for the next one; a short page ends the partition
	if !strings.HasSuffix(f.calls[2], "page=3") || !strings.Contains(f.calls[3], "countryCode=") {
		t.Errorf("calls = %v", f.calls[:4])
	}
}
