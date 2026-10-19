package prodir

// Total Immersion "Find a Coach", https://www.totalimmersion.net/get-coached/find-a-coach
// (~30 coach cards, individuals).
//
// A static Joomla page, one list and no paging, grouped under US-state and
// country headings. Each .find-box card has the coach's name, location
// ("City, ST" for the US, "City, Country" elsewhere), TI level, email, website
// and sometimes a phone. Coaches with a profile link ("/coach/{slug}", the id)
// also have a profile page that adds the bio (.post-content text), social links
// and the phone; a card without one (href "javascript:void(0)") gets the
// slugified name as its id and has no Detail. Country: a two-letter location
// ending is a US state, otherwise the last part of the location is the country
// (the page's own grouping); an unresolvable one stays as written.
//
// KNOWN GAPS: no street address, rates or languages.

import (
	"context"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	totalImmersionBase = "https://www.totalimmersion.net"
	totalImmersionList = totalImmersionBase + "/get-coached/find-a-coach"
)

var totalImmersionCountries = sync.OnceValue(newCountryResolver)

func init() {
	register(&Source{
		Slug: "totalimmersion", Name: "Total Immersion coaches", Host: "totalimmersion.net",
		Category: "swimming_coach", ListingType: "individual", MinInterval: time.Second,
		List: totalImmersionListFn, Detail: totalImmersionDetail,
	})
}

// totalImmersionPlace splits a location text into city, region and country.
func totalImmersionPlace(loc string) (city, region, country string) {
	var parts []string
	for _, p := range strings.Split(loc, ",") {
		if p = clean(p); p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) == 0 {
		return
	}
	last := parts[len(parts)-1]
	switch {
	case len(parts) > 1 && len(last) == 2 && last == strings.ToUpper(last): // "Phoenix, AZ"
		return strings.Join(parts[:len(parts)-1], ", "), last, "United States"
	case len(parts) > 1:
		return parts[0], strings.Join(parts[1:len(parts)-1], ", "), last
	}
	if _, code := totalImmersionCountries().resolve(last); code != "" { // a lone country
		return "", "", last
	}
	return last, "", ""
}

// totalImmersionCard reads the fields the list card and the profile share.
func totalImmersionCard(box *goquery.Selection) Record {
	r := Record{Meta: map[string][]string{}}
	icon := func(class string) *goquery.Selection { return box.Find("i." + class).First().Parent() }
	r.City, r.Region, r.Country = totalImmersionPlace(icon("fa-map-marker").Text())
	addMeta(r.Meta, "level", icon("fa-graduation-cap").Text())
	r.Emails = emailsIn(box)
	if a := icon("fa-link").Find("a[href]").First(); a.Length() > 0 {
		r.Websites = []string{clean(a.AttrOr("href", ""))}
	}
	for _, class := range []string{"fa-mobile", "fa-mobile-phone"} {
		if ph := clean(icon(class).Text()); ph != "" {
			r.Phones = []string{ph}
		}
	}
	return r
}

func totalImmersionListFn(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, base, err := getDoc(ctx, f, totalImmersionList)
	if err != nil {
		return err
	}
	cards := doc.Find(".find-box.find-box-image")
	if cards.Length() == 0 {
		return fmt.Errorf("no coach cards on %s (layout changed?)", base)
	}
	seen := map[string]bool{}
	var out []Record
	cards.Each(func(_ int, card *goquery.Selection) {
		a := card.Find("h3 a").First()
		name := clean(a.Text())
		if name == "" {
			return
		}
		r := totalImmersionCard(card)
		r.Name = name
		if href := absURL(base, a.AttrOr("href", "")); strings.Contains(href, "/coach/") {
			r.URL = href
			r.ID = strings.ToLower(href[strings.LastIndex(strings.TrimRight(href, "/"), "/")+1:])
		} else {
			r.ID = strings.ReplaceAll(slug(name), "_", "-")
		}
		r.ID = strings.TrimRight(r.ID, "/")
		if r.ID != "" && !seen[r.ID] { // the page repeats a coach under two headings
			seen[r.ID] = true
			out = append(out, r)
		}
	})
	for _, r := range out {
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

func totalImmersionDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	if r.URL == "" { // a card without a profile page: the list has everything
		return Record{}, nil
	}
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	box := doc.Find(".find-box.find-full").First()
	if box.Length() == 0 {
		return Record{}, fmt.Errorf("no coach block on %s (layout changed?)", base)
	}
	d := totalImmersionCard(box)
	bio := box.Parent().Clone()
	bio.Find(".find-box").Remove()
	d.Bio = clean(bio.Text())
	d.Social = map[string]string{}
	box.Find("a[href]").Each(func(_ int, a *goquery.Selection) {
		href := absURL(base, a.AttrOr("href", ""))
		if plat := socialPlatform(href); plat != "" && d.Social[plat] == "" {
			d.Social[plat] = href
		}
	})
	return d, nil
}
