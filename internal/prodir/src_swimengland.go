package prodir

// Swim England Poolfinder clubs, https://www.swimming.org/poolfinder/
// (~1,480 club/facility pages in the sitemap, organisations).
//
// List reads the poolfinder sitemap
// (/poolfinder/sitemap.xml, relative <loc> values) and keeps the
// /poolfinder/clubs/{clubId}/{facilityId}/{Name}/ entries; the id is
// "clubId-facilityId" (a club that trains at several pools has one page per
// pool). Each page is server-rendered: name (h1), address (facility, street,
// town, postcode), the club website, member count, abilities, disciplines and
// which disciplines are open to new members.
//
// KNOWN GAPS: profiles carry NO email, phone or social links, only the website,
// so follow-up goes through the website (enrich stage). The search page says
// "1,994 clubs" but the sitemap only lists ~970 distinct clubs / 1,476 pairs: it
// is stale. A sample of the (club, facility) pairs seen in the search is missing
// from the sitemap (some are extra facilities of listed clubs, some are clubs not
// listed at all). The search is "load more" paging that fails beyond ~150
// results, so it cannot be crawled; the sitemap is the only complete-enough
// enumeration. Swim England is the English national body: Country is the United
// Kingdom and the page's own country text (England) is the region.

import (
	"context"
	"fmt"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	swimEnglandBase    = "https://www.swimming.org"
	swimEnglandSitemap = swimEnglandBase + "/poolfinder/sitemap.xml"
)

var swimEnglandLocRe = regexp.MustCompile(`<loc>\s*(?:https?://[^/<]+)?(/poolfinder/clubs/(\d+)/(\d+)/([^<]*?))\s*</loc>`)

func init() {
	register(&Source{
		Slug: "swimengland", Name: "Swim England Poolfinder clubs", Host: "swimming.org",
		Category: "swimming_coach", ListingType: "club", MinInterval: time.Second,
		List: swimEnglandList, Detail: swimEnglandDetail,
	})
}

func swimEnglandList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, swimEnglandSitemap, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	locs := swimEnglandLocRe.FindAllSubmatch(p.Body, -1)
	if len(locs) == 0 {
		return fmt.Errorf("no club pages in the sitemap (layout changed?)")
	}
	seen := map[string]bool{}
	for _, m := range locs {
		id := string(m[2]) + "-" + string(m[3])
		if seen[id] {
			continue
		}
		seen[id] = true
		name := strings.Trim(string(m[4]), "/")
		if n, err := url.QueryUnescape(name); err == nil {
			name = n
		}
		r := Record{ID: id, URL: swimEnglandBase + html.UnescapeString(string(m[1])), Name: clean(name), Country: "United Kingdom"}
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// swimEnglandText is the text of a node without the trailing comma the page prints.
func swimEnglandText(s *goquery.Selection) string {
	return clean(strings.Trim(clean(s.Text()), ", "))
}

func swimEnglandDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	h1 := doc.Find(`h1[itemprop="name"]`).First()
	if h1.Length() == 0 {
		return Record{}, fmt.Errorf("no club name on %s (layout changed?)", base)
	}
	d := Record{Name: clean(h1.Text()), Country: "United Kingdom", Meta: map[string][]string{}}

	addr := doc.Find(`address[itemprop="address"]`).First()
	street := addr.Find(`[itemprop="streetAddress"]`).First().Clone()
	addMeta(d.Meta, "facility", street.Find("a").First().AttrOr("title", ""))
	street.Find("a").Remove() // the facility link; what is left is the street
	d.Address = clean(strings.Trim(clean(street.Text()), ", "))
	d.City = swimEnglandText(addr.Find(`[itemprop="addressLocality"]`))
	d.PostalCode = swimEnglandText(addr.Find(`[itemprop="postalCode"]`))
	d.Region = swimEnglandText(addr.Find(`[itemprop="addressCountry"]`))

	// the own site: schema.org sameAs, else the hero "Visit Website" link
	if w := clean(doc.Find(`[itemprop="sameAs"]`).First().Text()); w != "" {
		d.Websites = []string{w}
	} else if href := clean(doc.Find("nav.facility-hero-contact a[href]").First().AttrOr("href", "")); href != "" {
		d.Websites = []string{absURL(base, href)}
	}

	doc.Find("dl.club-information .club-information-point").Each(func(_ int, pt *goquery.Selection) {
		switch label := strings.ToLower(clean(pt.Find("dt").First().Clone().Children().Remove().End().Text())); label {
		case "no. of members":
			addMeta(d.Meta, "member_count", pt.Find("dd").Text())
		case "abilities":
			pt.Find("dd abbr").Each(func(_ int, a *goquery.Selection) { addMeta(d.Meta, "abilities", a.Text()) })
		}
	})
	doc.Find(".facility-attributes-container .badge").Each(func(_ int, b *goquery.Selection) {
		addMeta(d.Meta, "disciplines", b.Text())
	})
	doc.Find(".club-recruitment-container dl > div").Each(func(_ int, row *goquery.Selection) {
		if strings.EqualFold(clean(row.Find("dd").Text()), "yes") {
			addMeta(d.Meta, "open_to_new_members", row.Find("dt").Text())
		}
	})
	return d, nil
}
