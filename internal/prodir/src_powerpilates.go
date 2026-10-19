package prodir

// Power Pilates International Registry, https://powerpilates.com/instructors/
// (~24,400 currently certified instructors in ~37 countries, individuals).
//
// The index lists the countries (a.registry-country-card -> /instructors/{country}/).
// A country page is plain server-rendered HTML, 20 a.instructor-card per page,
// walked with ?page=N until a page has no "next" pagination link (or no cards).
// The default scope is the CURRENT certifications; the formerly certified
// (?show=all) are deliberately not walked. The profile (/instructors/profile/{slug}/)
// adds the bio, the optional Instagram link and the certification dates.
//
// Contact data: the registry has NO email, phone or website field (profiles
// only have a contact form). The value is name + city + country + certification
// level + bio + Instagram; a website or handle typed into the bio is picked up
// by the loader from Bio. The power-pilates brand's own social accounts are
// never taken as an instructor's. Country comes from the country the instructor
// is listed under (the card text of the index); the region from the profile's
// JSON-LD Person.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const ppBase = "https://powerpilates.com"

var (
	ppCountryRe = regexp.MustCompile(`^/instructors/([a-z0-9-]+)/?$`)
	ppIDRe      = regexp.MustCompile(`[^a-z0-9]+`)
)

func init() {
	register(&Source{
		Slug: "powerpilates", Name: "Power Pilates International Registry", Host: "powerpilates.com",
		Category: "pilates_instructor", ListingType: "individual", MinInterval: time.Second,
		List: ppList, Detail: ppDetail,
	})
}

type ppCountry struct{ slug, name string }

func ppList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, base, err := getDoc(ctx, f, ppBase+"/instructors/")
	if err != nil {
		return err
	}
	var countries []ppCountry
	seenC := map[string]bool{}
	doc.Find("a.registry-country-card").Each(func(_ int, a *goquery.Selection) {
		u, err := url.Parse(absURL(base, a.AttrOr("href", "")))
		if err != nil || u.Host != "powerpilates.com" {
			return
		}
		if m := ppCountryRe.FindStringSubmatch(u.Path); m != nil && !seenC[m[1]] {
			seenC[m[1]] = true
			countries = append(countries, ppCountry{m[1], clean(a.Find("span").First().Text())})
		}
	})
	if len(countries) == 0 {
		return fmt.Errorf("no countries on %s (layout changed?)", base)
	}

	seen := map[string]bool{}
	for _, c := range countries {
		for page := 1; page <= 1000; page++ {
			doc, base, err := getDoc(ctx, f, fmt.Sprintf("%s/instructors/%s/?page=%d", ppBase, c.slug, page))
			if err != nil {
				return err
			}
			cards := 0
			var recs []Record
			doc.Find("a.instructor-card").Each(func(_ int, card *goquery.Selection) {
				cards++
				if card.Find(".status-expired").Length() > 0 { // formerly certified (only listed with ?show=all)
					return
				}
				if r, ok := ppCard(card, base, c); ok {
					recs = append(recs, r)
				}
			})
			for _, r := range recs {
				if seen[r.ID] {
					continue
				}
				seen[r.ID] = true
				if err := emit(r); err != nil {
					return err
				}
			}
			if cards == 0 || !ppHasNext(doc, page) {
				break
			}
		}
	}
	return nil
}

// ppHasNext reports whether the pagination links to a page after page.
func ppHasNext(doc *goquery.Document, page int) bool {
	next := false
	doc.Find(`nav.pagination a[href*="page="]`).Each(func(_ int, a *goquery.Selection) {
		if u, err := url.Parse(a.AttrOr("href", "")); err == nil {
			if n, _ := strconv.Atoi(u.Query().Get("page")); n > page {
				next = true
			}
		}
	})
	return next
}

// ppSlug is the profile slug of a profile URL ("" when it is no profile URL).
func ppSlug(profileURL string) string {
	u, err := url.Parse(profileURL)
	if err != nil {
		return ""
	}
	s, ok := strings.CutPrefix(u.Path, "/instructors/profile/")
	if !ok {
		return ""
	}
	return strings.Trim(s, "/")
}

func ppCard(card *goquery.Selection, base string, c ppCountry) (Record, bool) {
	href := absURL(base, card.AttrOr("href", ""))
	// The slug is name-based and may hold a raw space: the ID is its DNS-safe form.
	id := strings.Trim(ppIDRe.ReplaceAllString(strings.ToLower(ppSlug(href)), "-"), "-")
	name := clean(card.Find(".instructor-name").Text())
	if id == "" || name == "" { // not a profile card
		return Record{}, false
	}
	r := Record{
		ID: id, URL: href, Name: name, Country: c.name,
		City: clean(card.Find(".instructor-location").Text()),
		Meta: map[string][]string{},
	}
	addMeta(r.Meta, "certification_level", card.Find(".instructor-cert").Text())
	return r, true
}

func ppDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	if doc.Find(".profile-header, .info-grid").Length() == 0 {
		return Record{}, fmt.Errorf("no profile on %s (layout changed?)", base)
	}
	d := Record{Meta: map[string][]string{}, Social: map[string]string{}}
	var bio []string
	doc.Find(".bio-section p").Each(func(_ int, p *goquery.Selection) {
		p.Find("br").AfterHtml(" ") // <br> must not glue two words together
		if t := clean(p.Text()); t != "" {
			bio = append(bio, t)
		}
	})
	d.Bio = strings.Join(bio, " ")

	doc.Find(".info-item").Each(func(_ int, it *goquery.Selection) {
		val := clean(it.Find(".info-content span").First().Text())
		switch strings.ToLower(clean(it.Find("label").First().Text())) {
		case "certification level":
			addMeta(d.Meta, "certification_level", val)
		case "time certified":
			addMeta(d.Meta, "time_certified", val)
		case "level achieved":
			addMeta(d.Meta, "level_achieved", val)
		case "certification expires":
			addMeta(d.Meta, "expires", val)
		}
	})
	addMeta(d.Meta, "certificate_number", doc.Find("span.cert-number").First().Text())
	if t := clean(doc.Find("header .title").First().Text()); t != "" {
		addMeta(d.Meta, "title", t)
	}

	doc.Find(".social-links a.social-icon").Each(func(_ int, a *goquery.Selection) {
		href := strings.TrimSpace(a.AttrOr("href", ""))
		p := socialPlatform(href)
		// the brand's own accounts (powerpilates, powerpilates_ny ...) are not the instructor's
		if p == "" || strings.Contains(strings.ToLower(href), "powerpilates") || d.Social[p] != "" {
			return
		}
		d.Social[p] = href
	})

	// JSON-LD Person: the region (state / province) of the instructor.
	doc.Find(`script[type="application/ld+json"]`).EachWithBreak(func(_ int, s *goquery.Selection) bool {
		var p struct {
			Type    string `json:"@type"`
			Address struct {
				Region string `json:"addressRegion"`
			} `json:"address"`
		}
		if json.Unmarshal([]byte(s.Text()), &p) != nil || p.Type != "Person" {
			return true
		}
		d.Region = clean(p.Address.Region)
		return d.Region == ""
	})
	return d, nil
}
