package prodir

// PGA (UK) find a golf lesson, https://www.pgaplay.co.uk/find-a-golf-lesson/
// (~1,790 PGA professionals, individuals).
//
// List reads the XML directory /xml-directory/ (one request), whose URLs are
// /find-a-golf-lesson/profile/{membershipNo}/{Name-Town}; the membership number
// is the stable id (a few names carry spaces, encoded as %20). The profile page has the name and address as ld+json, the
// pro's own phone (tel: link), the About text and, on some profiles, social
// links. Email is form-relay only (no mailto) and a website is rare. The
// ld+json streetAddress is often a venue name ("South Herts Golf Club"), so it
// is stored as the address as is. Country is GB (the directory is the PGA of
// Great Britain and Ireland's UK finder).

import (
	"context"
	"encoding/json"
	"fmt"
	"html"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	pgaUKBase    = "https://www.pgaplay.co.uk"
	pgaUKCountry = "GB" // the finder is the UK one; the profile ld+json repeats it
)

var pgaUKLocRe = regexp.MustCompile(`<loc>\s*(https://www\.pgaplay\.co\.uk/find-a-golf-lesson/profile/([A-Za-z0-9]+)/[^<]*?)\s*</loc>`)

// ldControlChars are raw line breaks inside ld+json strings, which JSON forbids.
var ldControlChars = strings.NewReplacer("\r", " ", "\n", " ", "\t", " ")

func init() {
	register(&Source{
		Slug: "pga-uk", Name: "PGA UK find a golf lesson", Host: "pgaplay.co.uk",
		Category: "sports_golf", ListingType: "individual", MinInterval: time.Second,
		List: pgaUKList, Detail: pgaUKDetail,
	})
}

func pgaUKList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, pgaUKBase+"/xml-directory/", nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, m := range pgaUKLocRe.FindAllSubmatch(p.Body, -1) {
		id := strings.ToLower(string(m[2]))
		if seen[id] {
			continue
		}
		seen[id] = true
		if err := emit(Record{ID: id, URL: strings.ReplaceAll(html.UnescapeString(string(m[1])), " ", "%20"), Country: pgaUKCountry}); err != nil {
			return err
		}
	}
	if len(seen) == 0 {
		return fmt.Errorf("no profile URLs in the XML directory (layout changed?)")
	}
	return nil
}

type pgaUKProfile struct {
	Name    string `json:"name"`
	Desc    string `json:"description"`
	Address struct {
		Street   string `json:"streetAddress"`
		Locality string `json:"addressLocality"`
		Postcode string `json:"postalCode"`
		Country  string `json:"addressCountry"`
	} `json:"address"`
}

func pgaUKDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	var ld pgaUKProfile
	found := false
	doc.Find(`script[type="application/ld+json"]`).Each(func(_ int, s *goquery.Selection) {
		var cand pgaUKProfile // the page also carries a breadcrumb ld+json: the profile is the one with an address
		if !found && json.Unmarshal([]byte(ldControlChars.Replace(s.Text())), &cand) == nil && cand.Name != "" && cand.Address.Locality+cand.Address.Postcode != "" {
			ld, found = cand, true
		}
	})
	if !found {
		return Record{}, fmt.Errorf("no ld+json profile on %s (layout changed?)", base)
	}
	d := Record{
		Name: clean(ld.Name), Address: clean(ld.Address.Street), City: clean(ld.Address.Locality),
		PostalCode: clean(ld.Address.Postcode), Country: firstNonEmpty(clean(ld.Address.Country), pgaUKCountry),
		Bio: firstNonEmpty(clean(doc.Find(".gl-bio").First().Text()), clean(ld.Desc)), Meta: map[string][]string{},
	}
	if tel := doc.Find(`a[href^="tel:"]`).First(); tel.Length() > 0 {
		if ph := structuredPhone(strings.TrimPrefix(tel.AttrOr("href", ""), "tel:")); ph != "" {
			d.Phones = []string{ph}
		}
	}
	doc.Find(".gl-social a[href]").Each(func(_ int, a *goquery.Selection) {
		if u := absURL(base, a.AttrOr("href", "")); socialPlatform(u) != "" {
			if d.Social == nil {
				d.Social = map[string]string{}
			}
			if d.Social[socialPlatform(u)] == "" {
				d.Social[socialPlatform(u)] = u
			}
		}
	})
	return d, nil
}
