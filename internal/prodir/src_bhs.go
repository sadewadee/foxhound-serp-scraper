package prodir

// British Horse Society "Find a riding centre or livery yard",
// https://www.bhs.org.uk/go-riding-and-learn/find-a-riding-centre-or-livery-yard/
// (~507 riding schools, livery yards and clubs, centers).
//
// The listing is a server-rendered card grid, 12 per page: ?page=N (1-based,
// ~43 pages); a page past the end has no cards and ends the walk. A card
// carries the name, "Town, Region" (England / Scotland / Wales / Northern
// Ireland, or a foreign place), the phone and service tags. The id is the
// relative slug of the card link, which is also the detail URL. The detail page
// adds a plain mailto email, the website, the free-text address and the "About
// Us" bio (the robots.txt-disallowed /api is never used).
//
// The directory has no country field. A UK region on the card or a UK suffix
// of the detail address ("..., UK") gives the United Kingdom; some centres are
// abroad (e.g. the UAE), whose address suffix names the country.

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const bhsList = "https://www.bhs.org.uk/go-riding-and-learn/find-a-riding-centre-or-livery-yard/"

func init() {
	register(&Source{
		Slug: "bhs", Name: "British Horse Society riding centres", Host: "www.bhs.org.uk",
		Category: "sports_equestrian", ListingType: "center", MinInterval: time.Second,
		List: bhsListFn, Detail: bhsDetail,
	})
}

var (
	bhsCountries = sync.OnceValue(newCountryResolver)
	bhsUKRegions = map[string]bool{"england": true, "scotland": true, "wales": true, "northern ireland": true}
	bhsPostcode  = regexp.MustCompile(`(?i)\b[A-Z]{1,2}\d[A-Z\d]? ?\d[A-Z]{2}$`)
)

// bhsItem is the value of the info-list row whose hidden icon label is label
// (phone, mail, website, map-marker).
func bhsItem(s *goquery.Selection, label string) *goquery.Selection {
	return s.Find("li.m-info-list__item").FilterFunction(func(_ int, li *goquery.Selection) bool {
		return clean(li.Find("span[hidden]").First().Text()) == label
	}).Find(".m-info-list__item__value").First()
}

func bhsListFn(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[string]bool{}
	for page := 1; page <= 200; page++ {
		doc, base, err := getDoc(ctx, f, fmt.Sprintf("%s?page=%d", bhsList, page))
		if err != nil {
			return err
		}
		cards := doc.Find("div.o-cards__card a.m-card--riding-centre[href]")
		if cards.Length() == 0 {
			return nil
		}
		var recs []Record
		cards.Each(func(_ int, a *goquery.Selection) {
			slug := strings.Trim(a.AttrOr("href", ""), "/ ")
			name := clean(a.Find(".m-card__heading").First().Text())
			// a card without slug or name is not a centre
			if slug == "" || name == "" || seen[slug] {
				return
			}
			seen[slug] = true
			r := Record{ID: slug, URL: absURL(base, slug), Name: name, Meta: map[string][]string{}}
			if loc := splitMeta(bhsItem(a, "map-marker").Text(), ","); len(loc) > 0 {
				r.City = clean(loc[0])
				if len(loc) > 1 {
					r.Region = clean(loc[len(loc)-1])
				}
			}
			if bhsUKRegions[strings.ToLower(r.Region)] {
				r.Country = "United Kingdom"
			}
			if p := clean(bhsItem(a, "phone").Text()); p != "" {
				r.Phones = []string{p}
			}
			a.Find(".m-tag-list li").Each(func(_ int, li *goquery.Selection) { addMeta(r.Meta, "services", clean(li.Text())) })
			recs = append(recs, r)
		})
		for _, r := range recs {
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

func bhsDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	if doc.Find("h1.o-hero__heading").Length() == 0 {
		return Record{}, fmt.Errorf("no centre heading on %s (layout changed?)", base)
	}
	d := Record{}
	root := doc.Selection
	if p := clean(bhsItem(root, "phone").Text()); p != "" {
		d.Phones = []string{p}
	}
	d.Emails = emailsIn(bhsItem(root, "mail"))
	if w := absURL(base, bhsItem(root, "website").Find("a[href]").First().AttrOr("href", "")); strings.HasPrefix(w, "http") {
		d.Websites = []string{w}
	}
	var bio []string
	doc.Find("#about-us p").Each(func(_ int, p *goquery.Selection) { bio = append(bio, clean(p.Text())) })
	d.Bio = clean(strings.Join(bio, " "))

	// "Unnamed Rd,, Peterculter AB14 0PN, UK": free text, last part = country
	panel := doc.Find(".m-info-panel-list").First().Clone()
	panel.Find("script, div").Remove()
	addr := clean(strings.ReplaceAll(panel.Text(), ",,", ","))
	if parts := splitMeta(addr, ","); addr != "" && len(parts) > 1 {
		d.Address = addr
		last := clean(parts[len(parts)-1])
		if name, _ := bhsCountries().resolve(last); name != "" {
			d.Country = name
		}
		if m := bhsPostcode.FindString(clean(parts[len(parts)-2])); m != "" && d.Country == "United Kingdom" {
			d.PostalCode = strings.ToUpper(m)
		}
	}
	if d.Country == "" && bhsUKRegions[strings.ToLower(r.Region)] {
		d.Country = "United Kingdom"
	}
	return d, nil
}
