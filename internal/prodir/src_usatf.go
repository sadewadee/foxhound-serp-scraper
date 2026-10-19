package prodir

// USATF coaches registry, https://usatf.sport80.com (widget 3, ~4,392
// registered coaches, individuals).
//
// A Sport80 public widget (see sport80.go), 44 list requests, no Detail. Rows
// are name and certifications only (no email, phone, address or website; the
// State exists only as a filter), so this is a seed list for SERP enrichment.
// The category is sports_running: USATF covers all of track and field, but the
// canonical set has no track-and-field bucket and running is the closest.
// The registry is the US federation's, so Country is "United States".

import (
	"context"
	"time"
)

func init() {
	register(&Source{
		Slug: "usatf", Name: "USATF coaches registry", Host: "usatf.sport80.com",
		Category: "sports_running", ListingType: "individual", MinInterval: time.Second,
		List: func(ctx context.Context, f Fetcher, emit func(Record) error) error {
			return sport80List(ctx, f, "usatf.sport80.com", 3, usatfRecord, emit)
		},
	})
}

// usatfCertifications are the registry's certificate columns that name a
// credential. CEU Courses and Career Accomplishments are per-course and
// per-result lists, not credentials, and are left out.
var usatfCertifications = []string{
	"USATF Specialty Courses", "Developmental Coach Program", "NFHS Certification",
	"NFHS + USATF (Discontinued)", "USTFCCCA Courses", "World Athletics Certificates", "RRCA Certification",
}

func usatfRecord(r s80Row) (Record, bool) {
	name := s80Text(r.Name)
	if name == "" { // not a real entry
		return Record{}, false
	}
	rec := Record{ID: clean(string(r.ID)), Name: name, Country: "United States", Meta: map[string][]string{}}
	addMeta(rec.Meta, "credential", r.info("USATF Level 1, 2 or 3 Type"))
	for _, c := range usatfCertifications {
		addMeta(rec.Meta, "certifications", r.info(c+" Type"))
	}
	return rec, true
}
