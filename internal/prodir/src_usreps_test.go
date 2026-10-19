package prodir

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"
)

// usrepsFake serves the registry form on GET and, on POST, the canned page for
// "org|state|prefix|page" (anything else is an empty result), since fakeFetcher
// ignores request bodies.
type usrepsFake struct {
	form  []byte
	pages map[string][]byte
	posts []string
}

func (f *usrepsFake) Get(_ context.Context, u string, _ map[string]string) (Page, error) {
	return Page{Status: 200, URL: u, Body: f.form}, nil
}

func (f *usrepsFake) Post(_ context.Context, u string, _ map[string]string, body []byte) (Page, error) {
	q, err := url.ParseQuery(string(body))
	if err != nil {
		return Page{}, err
	}
	key := strings.Join([]string{q.Get("organizationName"), q.Get("stateProvince"), q.Get("lastName"), q.Get("page")}, "|")
	f.posts = append(f.posts, key)
	b, ok := f.pages[key]
	if !ok {
		b = f.pages["empty"]
	}
	return Page{Status: 200, URL: u, Body: b}, nil
}

func TestUSREPSList(t *testing.T) {
	s := mustSource(t, "usreps")
	capped := readFixture(t, "usreps_capped_SYNTHETIC.html")
	f := &usrepsFake{
		form: readFixture(t, "usreps_form_SYNTHETIC.html"),
		pages: map[string][]byte{
			"empty":       readFixture(t, "usreps_empty_SYNTHETIC.html"),
			"ACE|AA||1":   readFixture(t, "usreps_ace_aa_p1_SYNTHETIC.html"),
			"ACE|AA||2":   readFixture(t, "usreps_ace_aa_p2_SYNTHETIC.html"),
			"ACE|BB||1":   capped, // cap: partition by last-name prefix
			"ACE|BB|a|1":  capped, // still capped: deepen again
			"ACE|BB|a-|1": readFixture(t, "usreps_nsca_bb_SYNTHETIC.html"),
			"ACE|BB|a-|2": readFixture(t, "usreps_empty_SYNTHETIC.html"),
		},
	}
	got := collect(t, s, f)
	meta := func(cred, num string) map[string][]string {
		return map[string][]string{"credential": {cred}, "credential_number": {num}, "expires": {"12/31/2030"}, "portable": {"No"}}
	}
	want := []Record{
		{ID: "ace-cpt-100001", Name: "Avery Testsurname", City: "Exampleton", Region: "AA", PostalCode: "10001", Country: "United States", Meta: meta("ACE CPT", "100001")},
		{ID: "ace-cpt-100002", Name: "Blake Fixture-Name", City: "Sampleford", Region: "AA", PostalCode: "10002", Country: "United States", Meta: meta("ACE CPT", "100002")},
		// an empty Country cell falls back to the registry's own country
		{ID: "ace-cpt-100003", Name: "Casey Placeholder", City: "Demoville", Region: "AA", PostalCode: "10003", Country: "United States", Meta: meta("ACE CPT", "100003")},
		// the same person with a second credential is a second record
		{ID: "ace-pt-100099", Name: "Avery Testsurname", City: "Exampleton", Region: "AA", PostalCode: "10001", Country: "United States", Meta: meta("ACE PT", "100099")},
		{ID: "nsca-cscs-200001", Name: "Dana Zed", City: "Nowhere", Region: "BB", PostalCode: "20001", Country: "United States", Meta: meta("NSCA CSCS", "200001")},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("records = %#v\nwant      %#v", got, want)
	}
	has := func(key string) bool {
		for _, p := range f.posts {
			if p == key {
				return true
			}
		}
		return false
	}
	for _, key := range []string{"ACE|AA||2", "ACE|BB|z|1", "ACE|BB|a'|1", "ACE|BB|a |1", "ACE|BB|a-|1", "NSCA|BB||1"} {
		if !has(key) {
			t.Errorf("missing request %q", key)
		}
	}
	if has("ACE|AA||3") || has("ACE|BB||2") {
		t.Errorf("paged past the last page / paged a capped query: %v", f.posts)
	}

	// emit's error (the -limit) stops the walk at once
	f.posts = nil
	err := s.List(context.Background(), f, func(Record) error { return errListLimit })
	if !errors.Is(err, errListLimit) || len(f.posts) != 1 {
		t.Errorf("limit: err=%v posts=%v", err, f.posts)
	}
}

func TestUSREPSName(t *testing.T) {
	for in, want := range map[string]string{"Smith, Jo": "Jo Smith", "  Cher ": "Cher", "De La Cruz, Ana Maria": "Ana Maria De La Cruz"} {
		if got := usrepsName(in); got != want {
			t.Errorf("usrepsName(%q) = %q, want %q", in, got, want)
		}
	}
}
