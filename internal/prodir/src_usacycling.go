package prodir

// USA Cycling coach finder, https://usacycling.sport80.com (widget 2, ~489
// certified coaches, individuals).
//
// A Sport80 public widget (see sport80.go): 5 list requests at 100 rows each
// carry everything the API exposes, so there is no Detail. The widget hides
// email, phone and address (displayEmail / displayPhone are off); a row has
// the coach's name, certification level, optional coaching specialities and,
// for about 40%, a website. The directory is the US national federation's and
// rows carry no country, so Country is "United States". Contact data has to
// come from the website (or SERP enrichment of the name).

import (
	"context"
	"time"
)

func init() {
	register(&Source{
		Slug: "usacycling", Name: "USA Cycling coach finder", Host: "usacycling.sport80.com",
		Category: "sports_cycling", ListingType: "individual", MinInterval: time.Second,
		List: func(ctx context.Context, f Fetcher, emit func(Record) error) error {
			return sport80List(ctx, f, "usacycling.sport80.com", 2, usacyclingRecord, emit)
		},
	})
}

func usacyclingRecord(r s80Row) (Record, bool) {
	name := s80Text(r.Name)
	if name == "" { // not a real entry
		return Record{}, false
	}
	rec := Record{ID: clean(string(r.ID)), Name: name, Country: "United States", Meta: map[string][]string{}}
	if w := r.info("Website"); w != "" {
		rec.Websites = []string{w}
	}
	addMeta(rec.Meta, "credential", r.info("Coach Certification Type"))
	addMeta(rec.Meta, "specialties", splitList(r.info("Coaching Specialities"))...)
	return rec, true
}
