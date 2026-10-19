package prodir

// LTA (Lawn Tennis Association, UK) Find a Coach,
// https://www.lta.org.uk/play/find-a-coach (~1,176 tennis coaches, individuals).
//
// Server-rendered cards at ?p=N, 10 per page; the last page is followed by a
// page with no cards. The card link carries the Salesforce record id, which is
// the stable id (the name segment of that link has spaces and must be
// %20-encoded to be fetched). The card holds qualification level, accreditation
// and venue names; the profile adds the gender and the accreditation renewal date.
// Anonymous HTML has NO email, phone, website, social links or street address
// (the contact button needs JS / login), so the records are seeds for SERP
// enrichment: name, qualification and venues. The directory is the UK
// association's, so Country is "United Kingdom".

import (
	"context"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const ltaBase = "https://www.lta.org.uk"

var ltaDistanceRe = regexp.MustCompile(`\s*\([\d.,]+ miles? away\)`)

func init() {
	register(&Source{
		Slug: "lta", Name: "LTA find a coach", Host: "lta.org.uk",
		Category: "sports_tennis", ListingType: "individual", MinInterval: time.Second,
		List: ltaList, Detail: ltaDetail,
	})
}

func ltaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[string]bool{}
	for page := 1; page <= 500; page++ {
		doc, base, err := getDoc(ctx, f, fmt.Sprintf("%s/play/find-a-coach?p=%d", ltaBase, page))
		if err != nil {
			return err
		}
		cards := doc.Find(".lta-card--coach")
		if cards.Length() == 0 { // past the last page
			return nil
		}
		var recs []Record
		cards.Each(func(_ int, card *goquery.Selection) {
			if r, ok := ltaCard(card, base); ok && !seen[r.ID] {
				seen[r.ID] = true
				recs = append(recs, r)
			}
		})
		for _, r := range recs {
			if err := emit(r); err != nil {
				return err
			}
		}
	}
	return nil
}

// ltaCard parses one coach card; ok is false for a card without a profile link.
func ltaCard(card *goquery.Selection, base string) (Record, bool) {
	href := absURL(base, card.Find("a[href*='coach-details/']").First().AttrOr("href", ""))
	_, tail, found := strings.Cut(href, "/coach-details/")
	id, name, _ := strings.Cut(tail, "/")
	if !found || id == "" {
		return Record{}, false
	}
	// a name segment with spaces (or a stray quote) must be percent-encoded to be fetched
	if n, err := url.PathUnescape(name); err == nil {
		name = n
	}
	r := Record{
		ID: id, URL: href[:strings.Index(href, "/coach-details/")] + "/coach-details/" + id + "/" + url.PathEscape(name),
		Name: clean(card.Find(".lta-card-title").First().Text()), Country: "United Kingdom",
		Meta: map[string][]string{},
	}
	card.Find(".lta-card-coach--label").Each(func(_ int, lab *goquery.Selection) {
		info := lab.Next()
		switch strings.ToLower(strings.TrimSuffix(clean(lab.Text()), ":")) {
		case "qualification":
			addMeta(r.Meta, "credential", ltaOwnText(info))
		case "accreditation":
			addMeta(r.Meta, "accreditation", ltaOwnText(info))
			info.Find("li").Each(func(_ int, li *goquery.Selection) { addMeta(r.Meta, "accredited_to", li.Text()) })
		case "location":
			info.Find("span").Each(func(_ int, sp *goquery.Selection) {
				addMeta(r.Meta, "venues", ltaDistanceRe.ReplaceAllString(clean(sp.Text()), ""))
			})
		}
	})
	return r, true
}

// ltaOwnText is the text of sel without its level meter and lists.
func ltaOwnText(sel *goquery.Selection) string {
	c := sel.Clone()
	c.Find("div, ul").Remove()
	return clean(c.Text())
}

func ltaDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	card := doc.Find(".p-coach-detail-card").First()
	if card.Length() == 0 {
		return Record{}, fmt.Errorf("no coach details on %s (layout changed?)", base)
	}
	d := Record{Meta: map[string][]string{}}
	card.Find(".u-caption1").Each(func(_ int, lab *goquery.Selection) {
		val := lab.Next()
		switch strings.ToLower(strings.TrimSuffix(clean(lab.Text()), ":")) {
		case "gender":
			addMeta(d.Meta, "gender", ltaOwnText(val))
		case "accreditation renewal date":
			addMeta(d.Meta, "accreditation_renewal", ltaOwnText(val))
		}
	})
	return d, nil
}
