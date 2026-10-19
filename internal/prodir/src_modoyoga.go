package prodir

// Modo Yoga studios, https://modoyoga.com/studio-search/ (~38 hot yoga studios
// in Canada, the US and France, listing_type studio).
//
// The studio search is ONE server-rendered Elementor page (~190 KB): an h2 per
// region ("Canadian Locations", "U.S. Locations", "International Locations"),
// an h3 per province / state (under International the h3 is the country), then
// one accordion item per studio with the studio name, the address lines and a
// "Visit Website" button. The button points at the studio's WordPress subsite
// (/<slug>); one studio uses its own domain, which is then its website and gets
// no detail request. Items marked "COMING SOON" are skipped (nothing to call).
//
// Contact fields: address, city, region, postal code from the accordion body
// (best effort: the city / region / postal parse needs the usual "City, PROV
// POSTAL" last line); country from the region heading, never guessed. Detail
// reads the studio's /<slug>/contact/ page: the phone is plain text (no tel:
// link) and the Instagram / Facebook links of the studio sit on that page. The
// subsite itself lives on modoyoga.com, so it is never a website. There is NO
// email: the contact page only has a form.
//
// Known gaps: two studios can share one subsite (Hamilton, Columbus), so they
// share the subsite's phone; the address on the subsite is not re-read.

import (
	"context"
	"html"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const modoListURL = "https://modoyoga.com/studio-search/"

func init() {
	register(&Source{
		Slug: "modoyoga", Name: "Modo Yoga studio search", Host: "modoyoga.com",
		Category: "hot_yoga_teacher", ListingType: "studio", MinInterval: time.Second,
		List: modoList, Detail: modoDetail,
	})
}

var (
	modoBrRe   = regexp.MustCompile(`(?i)<br\s*/?>`)
	modoTagRe  = regexp.MustCompile(`<[^>]*>`)
	modoSoonRe = regexp.MustCompile(`(?i)\*?\s*coming soon\s*\*?`)
	modoDashRe = regexp.MustCompile(`[^a-z0-9]+`)
	// "City, PROV POSTAL" (the last address line), else the same at the end of a
	// one-line address "street, unit, City, Province POSTAL".
	modoPostalRe  = regexp.MustCompile(`^([^,]+?),?\s+([A-Za-z]{2,3})\.?\s*,?\s*([A-Z]\d[A-Z] ?(?:\d[A-Z]\d)?|\d{5})$`)
	modoPostalRe2 = regexp.MustCompile(`,\s*([^,]+?),\s*([A-Za-z]{2,}\.?)\s*,?\s*([A-Z]\d[A-Z] ?(?:\d[A-Z]\d)?|\d{5})$`)
	modoPhoneRe   = regexp.MustCompile(`^\+?\(?\d[\d\s().\-]{7,}\d$`)
)

func modoSlug(s string) string {
	return strings.Trim(modoDashRe.ReplaceAllString(strings.ToLower(s), "-"), "-")
}

type modoStudio struct {
	title, href, country, region string
	intl                         bool // under "International Locations": the h3 is the country
	lines                        []string
}

func modoList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, base, err := getDoc(ctx, f, modoListURL)
	if err != nil {
		return err
	}
	var studios []modoStudio
	var country, region string
	international := false
	doc.Find("h2.elementor-heading-title, h3.elementor-heading-title, details.e-n-accordion-item").Each(func(_ int, s *goquery.Selection) {
		switch {
		case s.Is("h2"):
			switch t := strings.ToLower(clean(s.Text())); {
			case strings.Contains(t, "canadian"):
				country, region, international = "Canada", "", false
			case strings.Contains(t, "u.s."):
				country, region, international = "United States", "", false
			case strings.Contains(t, "international"):
				country, region, international = "", "", true
			}
		case s.Is("h3"):
			if international {
				country = clean(s.Text())
			} else {
				region = clean(s.Text())
			}
		default:
			title := clean(s.Find(".e-n-accordion-item-title-text").First().Text())
			href := strings.TrimSpace(s.Find("a.elementor-button-link").First().AttrOr("href", ""))
			// "COMING SOON" studios have nothing to visit or call yet.
			if title == "" || modoSoonRe.MatchString(title) || href == "" || href == "#" {
				return
			}
			var lines []string
			if raw, err := s.Find("p").First().Html(); err == nil {
				txt := html.UnescapeString(modoTagRe.ReplaceAllString(modoBrRe.ReplaceAllString(raw, "\n"), ""))
				for _, ln := range strings.Split(txt, "\n") {
					if ln = clean(ln); ln != "" {
						lines = append(lines, ln)
					}
				}
			}
			studios = append(studios, modoStudio{title, absURL(base, href), country, region, international, lines})
		}
	})

	// Two accordion items can share one subsite: they are then told apart by name.
	count := map[string]int{}
	for _, st := range studios {
		count[st.href]++
	}
	seen := map[string]bool{}
	for _, st := range studios {
		u, err := url.Parse(st.href)
		if err != nil {
			continue
		}
		own := u.Hostname() == "modoyoga.com" || u.Hostname() == "www.modoyoga.com"
		id := modoSlug(strings.Trim(u.Path, "/"))
		if !own || count[st.href] > 1 || id == "" {
			id = modoSlug(st.title)
		}
		if id == "" || seen[id] {
			continue
		}
		seen[id] = true

		r := Record{ID: id, Name: "Modo Yoga " + st.title, Region: st.region, Country: st.country}
		if own {
			r.URL = "https://modoyoga.com/" + strings.Trim(u.Path, "/") + "/"
		} else {
			r.URL = st.href
			r.Websites = []string{st.href}
		}
		if n := len(st.lines); n > 0 {
			r.Address = strings.Join(st.lines, ", ")
			m := modoPostalRe.FindStringSubmatch(st.lines[n-1])
			if m == nil {
				m = modoPostalRe2.FindStringSubmatch(st.lines[n-1])
			}
			if m != nil {
				r.City, r.PostalCode = m[1], m[3]
				if r.Region == "" && !st.intl { // abroad the code is the country's, not a region
					r.Region = m[2]
				}
			}
		}
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

func modoDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	if len(r.Websites) > 0 { // a studio on its own domain: no subsite to read
		return Record{}, nil
	}
	doc, _, err := getDoc(ctx, f, strings.TrimRight(r.URL, "/")+"/contact/")
	if err != nil {
		return Record{}, err
	}
	d := Record{Social: map[string]string{}}
	doc.Find(".elementor-widget-text-editor p").EachWithBreak(func(_ int, p *goquery.Selection) bool {
		if t := clean(p.Text()); modoPhoneRe.MatchString(t) {
			d.Phones = []string{t}
			return false
		}
		return true
	})
	doc.Find("a[href]").Each(func(_ int, a *goquery.Selection) {
		href := strings.TrimSpace(a.AttrOr("href", ""))
		p := socialPlatform(href)
		// the chain's own account (".../modoyoga/") is not the studio's
		if p == "" || !socialProfile(href) || d.Social[p] != "" || strings.HasSuffix(strings.TrimRight(strings.ToLower(href), "/"), "/modoyoga") {
			return
		}
		d.Social[p] = href
	})
	return d, nil
}
