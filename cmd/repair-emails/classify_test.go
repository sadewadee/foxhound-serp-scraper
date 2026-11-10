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
