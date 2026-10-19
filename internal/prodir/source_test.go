package prodir

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"
)

// fakeFetcher serves canned pages by URL (anything else is a 404) and records
// the requests, so a source's List / Detail run against a synthetic fixture.
type fakeFetcher struct {
	pages map[string]Page
	calls []string
}

func (f *fakeFetcher) Get(_ context.Context, url string, _ map[string]string) (Page, error) {
	f.calls = append(f.calls, url)
	p, ok := f.pages[url]
	if !ok {
		return Page{Status: 404, URL: url}, nil
	}
	if p.Status == 0 {
		p.Status = 200
	}
	if p.URL == "" {
		p.URL = url
	}
	return p, nil
}

func (f *fakeFetcher) Post(ctx context.Context, url string, hdr map[string]string, _ []byte) (Page, error) {
	return f.Get(ctx, url, hdr)
}

func okSource() *Source {
	return &Source{
		Slug: "reg-test", Name: "Registry test", Host: "reg-test-directory.example.org",
		Category: "yoga_teacher", ListingType: "individual",
		List: func(context.Context, Fetcher, func(Record) error) error { return nil },
	}
}

func TestSourceValidate(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Source)
		want string // "" = valid
	}{
		{"ok", func(*Source) {}, ""},
		{"ok sports category with NULL niche", func(s *Source) { s.Category = "sports_tennis" }, ""},
		{"bad slug", func(s *Source) { s.Slug = "Bad Slug" }, "invalid slug"},
		{"empty slug", func(s *Source) { s.Slug = "" }, "invalid slug"},
		{"reserved slug ace", func(s *Source) { s.Slug = "ace" }, "invalid slug"},
		{"reserved slug sources", func(s *Source) { s.Slug = "sources" }, "invalid slug"},
		{"duplicate slug", func(s *Source) { s.Slug = "iyengaruk" }, "duplicate slug"},
		{"missing name", func(s *Source) { s.Name = "" }, "missing Name"},
		{"missing host", func(s *Source) { s.Host = "" }, "Host"},
		{"host with scheme", func(s *Source) { s.Host = "https://example.org" }, "Host"},
		{"host with path", func(s *Source) { s.Host = "example.org/x" }, "Host"},
		{"upper-case host", func(s *Source) { s.Host = "Example.org" }, "Host"},
		{"unknown category", func(s *Source) { s.Category = "knitting" }, "unknown Category"},
		{"empty category", func(s *Source) { s.Category = "" }, "unknown Category"},
		{"unknown listing type", func(s *Source) { s.ListingType = "person" }, "unknown ListingType"},
		{"missing list", func(s *Source) { s.List = nil }, "missing List"},
	}
	for _, c := range cases {
		s := okSource()
		c.mut(s)
		err := s.validate()
		switch {
		case c.want == "" && err != nil:
			t.Errorf("%s: unexpected error %v", c.name, err)
		case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
			t.Errorf("%s: error = %v, want it to mention %q", c.name, err, c.want)
		}
	}
}

func TestRegister(t *testing.T) {
	ownBefore := len(extOwnDomains)
	s := okSource()
	register(s)
	t.Cleanup(func() {
		delete(registry, s.Slug)
		extOwnDomains = extOwnDomains[:ownBefore]
	})

	if got, ok := LookupSource("reg-test"); !ok || got != s {
		t.Fatalf("LookupSource = %v, %v", got, ok)
	}
	if !isOwnDomain(s.Host) || !isOwnDomain("www."+s.Host) {
		t.Error("registered Host must join the own-domain filter")
	}
	if isOwnDomain("someone-else.example.org") {
		t.Error("only registered hosts are own domains")
	}

	// the sort order of Sources() is by slug
	var slugs []string
	for _, x := range Sources() {
		slugs = append(slugs, x.Slug)
	}
	for i := 1; i < len(slugs); i++ {
		if slugs[i-1] >= slugs[i] {
			t.Errorf("Sources() not sorted: %v", slugs)
		}
	}

	defer func() {
		if recover() == nil {
			t.Error("registering a duplicate slug must panic")
		}
	}()
	register(okSource())
}

func TestRegisterPanicsOnInvalid(t *testing.T) {
	defer func() {
		if recover() == nil {
			t.Error("registering an invalid source must panic")
		}
	}()
	s := okSource()
	s.Category = "knitting"
	register(s)
}

func TestCategoryNicheMap(t *testing.T) {
	want := map[string]string{
		"pilates_instructor": "pilates", "yoga_teacher": "yoga", "hot_yoga_teacher": "yoga",
		"fitness_trainer": "fitness", "crossfit_trainer": "fitness", "swimming_coach": "fitness",
		"sports_martial_arts": "fitness",
		"sports_tennis":       "", "sports_golf": "", "sports_pickleball": "", "sports_running": "",
		"sports_triathlon": "", "sports_cycling": "", "sports_equestrian": "",
	}
	if len(categoryNiche) != len(want) {
		t.Errorf("categoryNiche has %d entries, want %d", len(categoryNiche), len(want))
	}
	for c, n := range want {
		if got, ok := categoryNiche[c]; !ok || got != n {
			t.Errorf("categoryNiche[%q] = %q (present %v), want %q", c, got, ok, n)
		}
	}
}

func TestRegisteredSourcesAreValidAndDistinct(t *testing.T) {
	for _, slug := range []string{"iyengaruk", "sharath", "crossfit-affiliates"} {
		if _, ok := LookupSource(slug); !ok {
			t.Errorf("source %s is not registered", slug)
		}
	}
}

func TestGate(t *testing.T) {
	g := &gate{every: 20 * time.Millisecond}
	t0 := time.Now()
	for i := 0; i < 4; i++ {
		if err := g.wait(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if d := time.Since(t0); d < 55*time.Millisecond {
		t.Errorf("4 slots took %v, want >= 60ms (3 gaps of 20ms)", d)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := (&gate{every: time.Hour, next: time.Now().Add(time.Hour)}).wait(ctx); err == nil {
		t.Error("wait must fail on a cancelled context")
	}
}

func TestPageErr(t *testing.T) {
	if err := (Page{Status: 200}).Err(); err != nil {
		t.Errorf("200: %v", err)
	}
	for _, st := range []int{404, 410} {
		if err := (Page{Status: st, URL: "u"}).Err(); !errors.Is(err, ErrNotFound) {
			t.Errorf("%d: %v, want ErrNotFound", st, err)
		}
	}
	if err := (Page{Status: 400, URL: "u"}).Err(); err == nil || errors.Is(err, ErrNotFound) {
		t.Errorf("400: %v, want a plain error", err)
	}
}
