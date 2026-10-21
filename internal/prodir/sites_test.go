package prodir

import (
	"reflect"
	"testing"
)

func TestNormalizeSiteURL(t *testing.T) {
	tests := []struct {
		in, url, host string
		ok            bool
	}{
		{"https://www.sweatwithmeg.com/", "https://www.sweatwithmeg.com/", "www.sweatwithmeg.com", true},
		{"  sweatwithmeg.com  ", "https://sweatwithmeg.com/", "sweatwithmeg.com", true},
		{"http://Example.COM/About#team", "http://example.com/About", "example.com", true},
		{"//example.com/x", "https://example.com/x", "example.com", true},
		{"ftp://example.com", "", "", false},
		{"mailto:me@example.com", "", "", false},
		{"localhost", "", "", false},
		{"see my site", "", "", false},
		{"", "", "", false},
	}
	for _, tc := range tests {
		u, h, ok := normalizeSiteURL(tc.in)
		if u != tc.url || h != tc.host || ok != tc.ok {
			t.Errorf("normalizeSiteURL(%q) = %q, %q, %v; want %q, %q, %v", tc.in, u, h, ok, tc.url, tc.host, tc.ok)
		}
	}
}

func TestSiteSkipReason(t *testing.T) {
	tests := []struct{ in, dir, want string }{
		{"https://www.acefitness.org/fitness-professionals/x", "acefitness.org", "own"},
		{"https://apps.coachingfederation.org/eweb/x", "coachingfederation.org", "own"},
		{"https://www.facebook.com/someone", "acefitness.org", "social"},
		{"https://linktr.ee/someone", "acefitness.org", "blocked"},
		{"https://maps.app.goo.gl/abc", "acefitness.org", "blocked"},
		{"https://shop.calendly.com/x", "acefitness.org", "blocked"},
		{"https://notcalendly.com/", "acefitness.org", ""},
		{"https://www.sweatwithmeg.com/", "acefitness.org", ""},
	}
	for _, tc := range tests {
		_, host, ok := normalizeSiteURL(tc.in)
		if !ok {
			t.Fatalf("%q did not normalise", tc.in)
		}
		if got := siteSkipReason(tc.in, host, tc.dir); got != tc.want {
			t.Errorf("siteSkipReason(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

func TestSiteCollector(t *testing.T) {
	st := &SiteStats{}
	claimed := map[string]bool{"taken.com": true}
	c := newSiteCollector("acefitness.org", st, claimed)
	for _, raw := range []string{
		"https://www.acefitness.org/pro/1", // own host
		"https://instagram.com/x",          // social
		"linktr.ee/x",                      // blocked
		"not a url",                        // invalid
		"",                                 // ignored, not a candidate
		"sweatwithmeg.com/about",           // no scheme, deep page
		"https://www.sweatwithmeg.com/",    // same host (www), root wins
		"https://www.sweatwithmeg.com/",    // same URL again
		"https://other.com/page",           // first for host
		"https://other.com/page2",          // second deep page: first stays
		"https://taken.com/",               // claimed by an earlier source
	} {
		c.add(raw)
	}
	wantURLs := []string{"https://www.sweatwithmeg.com/", "https://other.com/page"}
	if !reflect.DeepEqual(c.urls, wantURLs) {
		t.Errorf("urls = %v, want %v", c.urls, wantURLs)
	}
	want := SiteStats{Candidates: 10, Invalid: 1, OwnHost: 1, Social: 1, Blocked: 1, DupURL: 1, DupHost: 3}
	if *st != want {
		t.Errorf("stats = %+v, want %+v", *st, want)
	}
}

func TestSiteSources(t *testing.T) {
	if s, err := siteSources("ace"); err != nil || len(s) != 1 || s[0].domainLike != "%.pro.acefitness.org" {
		t.Errorf("ace: %v %v", s, err)
	}
	if s, err := siteSources("all"); err != nil || len(s) < 2 {
		t.Errorf("all: %v %v", s, err)
	}
	if _, err := siteSources("nope"); err == nil {
		t.Error("unknown source should fail")
	}
}
