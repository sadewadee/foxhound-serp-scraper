package prodir

// USA Judo, https://usajudo.sport80.com: the club locator (widget 1, ~491
// clubs) as source usajudo and the certified coaches list (widget 3, ~1,313
// coaches, individuals) as source usajudo-coaches.
//
// Sport80 public widgets (see sport80.go), 5 and 14 list requests, no Detail.
// A club row has the name, a free-text address ("street, city, state, country,
// zip"), phone, email, website (~67%) and weekly class schedules. A coach row
// has only the name, the state association, the club and the certification,
// so it is a seed list; its club name goes to Business. Rows carry the country
// in the address (clubs) or not at all (coaches); the national federation is
// American, so coaches default to "United States".

import (
	"context"
	"strings"
	"time"
)

func init() {
	register(&Source{
		Slug: "usajudo", Name: "USA Judo club locator", Host: "usajudo.sport80.com",
		Category: "sports_martial_arts", ListingType: "club", MinInterval: time.Second,
		List: func(ctx context.Context, f Fetcher, emit func(Record) error) error {
			return sport80List(ctx, f, "usajudo.sport80.com", 1, usajudoClub, emit)
		},
	})
	register(&Source{
		Slug: "usajudo-coaches", Name: "USA Judo certified coaches", Host: "usajudo.sport80.com",
		Category: "sports_martial_arts", ListingType: "individual", MinInterval: time.Second,
		List: func(ctx context.Context, f Fetcher, emit func(Record) error) error {
			return sport80List(ctx, f, "usajudo.sport80.com", 3, usajudoCoach, emit)
		},
	})
}

// judoAddress splits "street[, more street], city, state, country, zip" from
// the end; a shorter address keeps what it has and the rest is the street.
func judoAddress(addr string) (street, city, region, country, zip string) {
	parts := splitList(addr)
	pop := func() string {
		if len(parts) == 0 {
			return ""
		}
		v := parts[len(parts)-1]
		parts = parts[:len(parts)-1]
		return v
	}
	if len(parts) < 5 { // too short to name its parts reliably: all street
		return strings.Join(parts, ", "), "", "", "", ""
	}
	zip, country, region, city = pop(), pop(), pop(), pop()
	return strings.Join(parts, ", "), city, region, country, zip
}

func usajudoClub(r s80Row) (Record, bool) {
	name := s80Text(r.Name)
	if name == "" { // not a real entry
		return Record{}, false
	}
	street, city, region, country, zip := judoAddress(s80Text(r.Address))
	rec := Record{
		ID: clean(string(r.ID)), Name: name,
		Address: street, City: city, Region: region, PostalCode: zip, Country: firstNonEmpty(country, "United States"),
		Emails: structuredEmails(r.info("Email Address")), Meta: map[string][]string{},
	}
	if p := structuredPhone(r.info("Phone Number")); p != "" {
		rec.Phones = []string{p}
	}
	if w := r.info("Organization Website"); w != "" {
		rec.Websites = []string{w}
	}
	addMeta(rec.Meta, "state_association", r.info("State"))
	addMeta(rec.Meta, "membership", r.info("Club Membership Type"))
	// class schedules are free text: they go to Bio, where contacts in them are found at load
	var bio []string
	for _, level := range []string{"Senior", "Junior", "Combined", "Other Classes"} {
		if days := r.info(level); days != "" {
			addMeta(rec.Meta, "classes", level)
			bio = append(bio, level+": "+days)
		}
		if more := r.info(level + " - Additional Information"); more != "" {
			bio = append(bio, level+" schedule: "+more)
		}
	}
	rec.Bio = strings.Join(bio, ". ")
	return rec, true
}

func usajudoCoach(r s80Row) (Record, bool) {
	name := s80Text(r.Name)
	if name == "" { // not a real entry
		return Record{}, false
	}
	rec := Record{
		ID: clean(string(r.ID)), Name: name, Business: r.info("Club"), Country: "United States",
		Meta: map[string][]string{},
	}
	addMeta(rec.Meta, "credential", r.info("Coach Certification Type"))
	addMeta(rec.Meta, "state_association", r.info("State"))
	addMeta(rec.Meta, "membership", r.info("Individual Membership Type"))
	addMeta(rec.Meta, "cert_expires", r.info("Exp Date"))
	addMeta(rec.Meta, "membership_valid_to", r.info("Valid To"))
	return rec, true
}
