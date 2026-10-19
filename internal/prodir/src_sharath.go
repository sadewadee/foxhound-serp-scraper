package prodir

// Sharath Yoga Centre authorised teachers, https://sharathyogacentre.com/authorised-teachers-directory/
// (~990 Ashtanga teachers, individuals).
//
// A Business Directory (wpbdp) plugin site. The "all listings" view
// (?wpbdp_view=all_listings) holds every teacher on ONE page (~2 MB): name,
// authorisation level, shala, city, state and country per card. The profile
// adds email (mailto link written as HTML entities), phone, website and the
// postal address. The profile page footer carries the centre's OWN contact
// details, so Detail only reads the entry's .listing-details block.

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const sharathListURL = "https://sharathyogacentre.com/authorised-teachers-directory/?wpbdp_view=all_listings"

var sharathIDRe = regexp.MustCompile(`\bwpbdp-listing-(\d+)\b`)

func init() {
	register(&Source{
		Slug: "sharath", Name: "Sharath Yoga Centre authorised teachers", Host: "sharathyogacentre.com",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: sharathList, Detail: sharathDetail,
	})
}

// wpbdpPlain are the field classes that name no field of their own.
var wpbdpPlain = map[string]bool{"display": true, "value": true, "meta": true, "tags": true, "title": true}

// wpbdpFields maps the plugin's field keys (class "wpbdp-field-<key>") to
// their value elements.
func wpbdpFields(box *goquery.Selection) map[string]*goquery.Selection {
	out := map[string]*goquery.Selection{}
	box.Find(".wpbdp-field-display").Each(func(_ int, fld *goquery.Selection) {
		for _, c := range strings.Fields(fld.AttrOr("class", "")) {
			k, ok := strings.CutPrefix(c, "wpbdp-field-")
			if ok && !wpbdpPlain[k] && !strings.HasPrefix(k, "type-") && !strings.HasPrefix(k, "association-") {
				out[k] = fld.Find(".value").First()
			}
		}
	})
	return out
}

func sharathList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, base, err := getDoc(ctx, f, sharathListURL)
	if err != nil {
		return err
	}
	var out []Record
	doc.Find("div.wpbdp-listing").Each(func(_ int, card *goquery.Selection) {
		m := sharathIDRe.FindStringSubmatch(card.AttrOr("class", ""))
		a := card.Find(".listing-title a").First()
		if m == nil || a.Length() == 0 {
			return
		}
		r := Record{ID: m[1], URL: absURL(base, a.AttrOr("href", "")), Name: clean(a.Text()), Meta: map[string][]string{}}
		sharathFields(&r, wpbdpFields(card))
		out = append(out, r)
	})
	for _, r := range out {
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// sharathFields copies the fields both the card and the profile show.
func sharathFields(r *Record, fld map[string]*goquery.Selection) {
	text := func(k string) string {
		if s := fld[k]; s != nil {
			return clean(s.Text())
		}
		return ""
	}
	addMeta(r.Meta, "authorisation_level", text("authorisation_level"))
	r.Business = text("shala_name")
	r.City = text("city")
	r.Region = text("state")
	r.Country = text("country")
}

func sharathDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	box := doc.Find("div.wpbdp-listing.single .listing-details").First()
	if box.Length() == 0 {
		return Record{}, fmt.Errorf("no listing details on %s (layout changed?)", base)
	}
	fld := wpbdpFields(box)
	d := Record{Meta: map[string][]string{}}
	sharathFields(&d, fld)

	text := func(k string) string {
		if s := fld[k]; s != nil {
			return clean(s.Text())
		}
		return ""
	}
	if s := fld["email"]; s != nil {
		d.Emails = emailsIn(s)
		for _, e := range structuredEmails(s.Text()) {
			d.Emails = addString(d.Emails, e)
		}
	}
	if s := fld["phone"]; s != nil {
		d.Phones = []string{firstNonEmpty(strings.TrimPrefix(s.Find("a[href^='tel:']").AttrOr("href", ""), "tel:"), clean(s.Text()))}
	}
	if s := fld["website"]; s != nil {
		d.Websites = []string{firstNonEmpty(absURL(base, s.Find("a").AttrOr("href", "")), clean(s.Text()))}
	}
	var addr []string
	for _, k := range []string{"address_line_1", "address_line_2"} {
		if v := strings.Trim(text(k), " ,"); v != "" {
			addr = append(addr, v)
		}
	}
	d.Address = strings.Join(addr, ", ")
	d.PostalCode = text("zip_code")
	return d, nil
}
