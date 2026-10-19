package prodir

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"testing"
)

// bodycontrolFetcher answers the name-search POSTs by the searched letter.
type bodycontrolFetcher struct {
	byLetter map[string][]byte
	none     []byte
	letters  []string
	hdrs     []map[string]string
}

func (b *bodycontrolFetcher) Get(context.Context, string, map[string]string) (Page, error) {
	return Page{Status: 404}, nil
}

func (b *bodycontrolFetcher) Post(_ context.Context, u string, hdr map[string]string, body []byte) (Page, error) {
	q, err := url.ParseQuery(string(body))
	if err != nil || q.Get("searchtype") != "name" || u != bodycontrolURL {
		return Page{Status: 400, URL: u}, nil
	}
	b.letters = append(b.letters, q.Get("name"))
	b.hdrs = append(b.hdrs, hdr)
	if p, ok := b.byLetter[q.Get("name")]; ok {
		return Page{Status: 200, URL: u, Body: p}, nil
	}
	return Page{Status: 200, URL: u, Body: b.none}, nil
}

func TestBodyControlList(t *testing.T) {
	s := mustSource(t, "bodycontrol")
	f := &bodycontrolFetcher{
		byLetter: map[string][]byte{
			"a": readFixture(t, "bodycontrol_results_a_SYNTHETIC.html"),
			"e": readFixture(t, "bodycontrol_results_e_SYNTHETIC.html"),
		},
		none: readFixture(t, "bodycontrol_none_SYNTHETIC.html"),
	}
	got := collect(t, s, f)
	want := []Record{
		{ID: "90001", Name: "Alex Fixture", Address: "Example Pilates Studio, 1 Example Road, Fixtureside",
			City: "Exampleton", PostalCode: "EX1 2AB", Country: "GB", Phones: []string{"01632 960001"},
			Emails: []string{"alex.fixture@example.org"}, Websites: []string{"https://alex-fixture.example.com/"},
			Social: map[string]string{},
			Meta:   map[string][]string{"credential": {"Matwork", "Reformer", "Hypermobility", "Pre- & Postnatal"}}},
		// the directory's own link is dropped, facebook becomes Social, two phones
		{ID: "90002", Name: "Blair Placeholder", Address: "Sample Lane", City: "Sampleford", PostalCode: "SF1 9ZZ",
			Country: "GB", Phones: []string{"01632 960002", "01632 960003"},
			Emails: []string{"blair.placeholder@example.com"},
			Social: map[string]string{"facebook": "https://www.facebook.com/blair.placeholder"},
			Meta:   map[string][]string{"credential": {"Matwork"}}},
		// ISO-3 PRT -> PT, foreign postcode, no phone, no website
		{ID: "90003", Name: "Cameron Overseas", Address: "Rua Exemplo 5", City: "Lisboa", PostalCode: "1000-001",
			Country: "PT", Emails: []string{"cameron.overseas@example.org"}, Social: map[string]string{},
			Meta: map[string][]string{"credential": {"Reformer"}}},
		// 90002 repeats in "e" and is deduped; an unknown country code stays empty and the
		// non-postcode line stays in the address
		{ID: "90004", Name: "Dana Unknown", Address: "Somewhere, Elsewhere", Emails: []string{"dana.unknown@example.org"},
			Social: map[string]string{}, Meta: map[string][]string{"credential": {"Matwork"}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	// the whole alphabet is swept, in order, with the form content type
	if len(f.letters) != 26 || f.letters[0] != "a" || f.letters[25] != "z" {
		t.Errorf("letters = %v", f.letters)
	}
	if f.hdrs[0]["Content-Type"] != "application/x-www-form-urlencoded" {
		t.Errorf("headers = %v", f.hdrs[0])
	}
	if s.MinInterval.Seconds() != 10 {
		t.Errorf("MinInterval = %v, robots.txt asks for a 10 s crawl delay", s.MinInterval)
	}

	// -limit stops the sweep at once
	f.letters = nil
	if err := s.List(context.Background(), f, func(Record) error { return errListLimit }); !errors.Is(err, errListLimit) || len(f.letters) != 1 {
		t.Errorf("limit: err=%v letters=%v", err, f.letters)
	}
	// an HTTP error fails the stage
	if err := s.List(context.Background(), &fakeFetcher{}, func(Record) error { return nil }); err == nil {
		t.Error("HTTP 404 on the search must fail List")
	}
}

func TestBodyControlListing(t *testing.T) {
	s := mustSource(t, "bodycontrol")
	l := recordListing(s, Record{ID: "90001", Name: "Alex Fixture", Country: "GB", Emails: []string{"a@example.org"}}, newCountryResolver())
	if l.CountryCode != "GB" || l.Niche != "pilates" {
		t.Errorf("country=%q niche=%q", l.CountryCode, l.Niche)
	}
}
