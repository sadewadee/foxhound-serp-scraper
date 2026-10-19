package prodir

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"
)

// pfFetcher serves a results page per query point; points not in pages
// answer "no center" (an unplaceable query), points in missing answer 404.
type pfFetcher struct {
	pages   map[string][]byte
	missing map[string]bool
	nocntr  []byte
	asked   []string
}

func (p *pfFetcher) Get(_ context.Context, u string, _ map[string]string) (Page, error) {
	pu, err := url.Parse(u)
	if err != nil || pu.Scheme+"://"+pu.Host+pu.Path != pfSearchURL {
		return Page{Status: 400, URL: u}, nil
	}
	pt := pu.Query().Get("address")
	p.asked = append(p.asked, pt)
	switch {
	case p.missing[pt]:
		return Page{Status: 404, URL: u}, nil
	case p.pages[pt] != nil:
		return Page{Status: 200, URL: u, Body: p.pages[pt]}, nil
	}
	return Page{Status: 200, URL: u, Body: p.nocntr}, nil
}

func (p *pfFetcher) Post(ctx context.Context, u string, h map[string]string, _ []byte) (Page, error) {
	return p.Get(ctx, u, h)
}

func TestPilatesFoundationList(t *testing.T) {
	s := mustSource(t, "pilatesfoundation")
	f := &pfFetcher{
		pages: map[string][]byte{
			"AB10": readFixture(t, "pilatesfoundation_AB10_SYNTHETIC.html"),
			"AL1":  readFixture(t, "pilatesfoundation_AL1_SYNTHETIC.html"),
		},
		missing: map[string]bool{"B1": true},
		nocntr:  readFixture(t, "pilatesfoundation_nocenter_SYNTHETIC.html"),
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "60001", Name: "Alex Fixture", Business: "Example Pilates Studio", PostalCode: "AB10 1AA", Country: "United Kingdom",
			Emails: []string{"alex.fixture@example.org"}, Phones: []string{"01632 960001"},
			Websites: []string{"https://alex-fixture.example.com"},
			URL:      "https://www.pilatesfoundation.com/teacher-profile?u=60001"},
		// business name equal to the name is not a business; two numbers; no website
		{ID: "60002", Name: "Blair Placeholder", PostalCode: "AB11 6ZZ", Country: "United Kingdom",
			Emails: []string{"blair.placeholder@example.com"}, Phones: []string{"07700 900002", "07700 900003"}},
		// 60003 is inactive; 60002 repeats in the next circle and is deduped
		{ID: "60004", Name: "Cameron Sample", Business: "Sample Body Studio", PostalCode: "AL1 1AA", Country: "United Kingdom",
			Emails: []string{"cameron.sample@example.org"}, Websites: []string{"http://www.sample-body.example.org"}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// every grid point is queried once, in order
	if !reflect.DeepEqual(f.asked, pfGrid) {
		t.Errorf("asked %d points, grid has %d", len(f.asked), len(pfGrid))
	}

	// -limit stops the walk at the first record
	f.asked = nil
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); !errors.Is(err, errListLimit) || len(f.asked) != 1 {
		t.Errorf("limit: err=%v asked=%d", err, len(f.asked))
	}
	// a layout change (no PF_INITIAL anywhere) fails; a server error fails
	none := &pfFetcher{pages: map[string][]byte{}, nocntr: []byte("<html>nothing</html>")}
	if err := s.List(context.Background(), none, func(Record) error { return nil }); err == nil {
		t.Error("no PF_INITIAL anywhere must fail List")
	}
	if err := s.List(context.Background(), &fakeFetcher{pages: map[string]Page{}}, func(Record) error { return nil }); err == nil {
		t.Error("a 400 must fail List")
	}
}

func TestPilatesFoundationGrid(t *testing.T) {
	if n := len(pfGrid); n < 100 || n > 200 {
		t.Errorf("grid has %d points, want a bounded ~150", n)
	}
	seen := map[string]bool{}
	for _, p := range pfGrid {
		if p == "" || seen[p] {
			t.Errorf("empty or duplicate grid point %q", p)
		}
		seen[p] = true
	}
}
