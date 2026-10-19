package prodir

// USA Taekwondo club locator, https://usataekwondo.sport80.com (widget 1, ~439
// clubs).
//
// A Sport80 public widget (see sport80.go), 5 list requests, no Detail. A row
// has the club name, a plain email, a website (~95%) and the state association
// or federation; the telephone and address fields are null on every row. The
// directory is the US national governing body's, rows carry no country, so
// Country is "United States".

import (
	"context"
	"time"
)

func init() {
	register(&Source{
		Slug: "usatkd", Name: "USA Taekwondo club locator", Host: "usataekwondo.sport80.com",
		Category: "sports_martial_arts", ListingType: "club", MinInterval: time.Second,
		List: func(ctx context.Context, f Fetcher, emit func(Record) error) error {
			return sport80List(ctx, f, "usataekwondo.sport80.com", 1, usatkdRecord, emit)
		},
	})
}

func usatkdRecord(r s80Row) (Record, bool) {
	name := s80Text(r.Name)
	if name == "" { // not a real entry
		return Record{}, false
	}
	rec := Record{
		ID: clean(string(r.ID)), Name: name, Country: "United States",
		Emails: structuredEmails(s80Text(r.Email)), Meta: map[string][]string{},
	}
	if w := r.info("Website"); w != "" {
		rec.Websites = []string{w}
	}
	addMeta(rec.Meta, "state_association", r.info("State"))
	addMeta(rec.Meta, "membership", r.info("Club Membership Type"))
	return rec, true
}
