package main

import (
	"testing"

	"github.com/sadewadee/serp-scraper/internal/emailfix"
)

func TestClassifySplit(t *testing.T) {
	cases := []struct {
		name, in, fixed, action string
	}{
		{"two addresses split", "anna@studio-a.de; office@studio-a.de", "anna@studio-a.de;office@studio-a.de", actSplit},
		{"only valid pieces kept", "anna@x-a.de, .kowalski@uni-a.pl", "anna@x-a.de", actSplit},
		{"all pieces invalid rejected", ".kowalski@uni-a.pl; ..jdoe1977@aol.com", ".kowalski@uni-a.pl; ..jdoe1977@aol.com", actReject},
		{"double at rejected", "info@@studio-a.de", "info@@studio-a.de", actReject},
		{"encoded comma split", "ab@x-a.com,%20cd@y-b.org", "ab@x-a.com;cd@y-b.org", actSplit},
		{"one letter local with bad tld rejected", "c@-.at", "c@-.at", actReject},
		{"two-char split with encoded spaces", "ab@one-fixture.com;%20%20cd@two-fixture.org", "ab@one-fixture.com;cd@two-fixture.org", actSplit},
		{"br join", "ab@one-fixture.com <br>cd@two-fixture.org", "ab@one-fixture.com;cd@two-fixture.org", actSplit},
		{"encoded space around lub", "ab@one-fixture.com%20lub%20cd.e@two-fixture.org", "ab@one-fixture.com;cd.e@two-fixture.org", actSplit},
		{"cc join", "ab@one-fixture.com&cc=cd@two-fixture.org", "ab@one-fixture.com;cd@two-fixture.org", actSplit},
		{"colon repeat keeps one address", "ab@one-fixture.com:ab@one-fixture.com", "ab@one-fixture.com", actSplit},
		{"quote-gt repeat keeps one address", "ab@one-fixture.com\">ab@one-fixture.com", "ab@one-fixture.com", actSplit},
		{"space after letters rejected", "mary beth smith msmith@gmail.com", "mary beth smith msmith@gmail.com", actReject},
		{"prefix then space rejected", "mailto:prayerful touch massage@gmail.com", "mailto:prayerful touch massage@gmail.com", actReject},
		{"two at signs glued rejected", "info@pilatesfixture@gmail.com", "info@pilatesfixture@gmail.com", actReject},
		{"role at role rejected", "info@sales@fixture-mon.com", "info@sales@fixture-mon.com", actReject},
		{"doubled domain rejected", "ab@fixture.dekb@fixture.de", "ab@fixture.dekb@fixture.de", actReject},
		{"space in domain rejected", "x@sattva samadhi.com", "x@sattva samadhi.com", actReject},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			fixed, action, rules := classify(c.in, emailfix.Context{})
			if action != c.action {
				t.Fatalf("classify(%q) action = %q, want %q (fixed %q, rules %v)", c.in, action, c.action, fixed, rules)
			}
			if c.action == actSplit && fixed != c.fixed {
				t.Errorf("classify(%q) fixed = %q, want %q", c.in, fixed, c.fixed)
			}
			if c.action == actReject && len(rules) == 0 {
				t.Errorf("classify(%q) reject without rules: a rejected row must carry a rule so it is marked malformed", c.in)
			}
		})
	}
}
