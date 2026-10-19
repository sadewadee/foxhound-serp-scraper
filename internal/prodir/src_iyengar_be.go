package prodir

// Belgian Iyengar Yoga Association (find a teacher), https://iyengaryoga.be/en/find-a-teacher
// (~72 teachers, individuals).
//
// One server-rendered page lists every teacher as a div.teacher-bloc. The
// filter form (GET: zip, country BE|LU, terms) has no effect on the plain GET,
// and the page already includes the Luxembourg teachers, so List is a single
// request. A block gives the name (strong), an optional website link, the
// email as text, a tel: link and one list item per studio. The email is
// hidden between decoy spans (span.blockspam holds random text, different on
// every load): List drops them and reads what is left. Each studio item links
// to a Google Maps place whose query encodes "street, postcode, city,
// country"; the first one gives the address and the country (Belgium when the
// teacher has no address). There is no id: it is the name slug, plus a hash
// of the first address when two teachers share a name. No level, social or bio.

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const iyengarBEURL = "https://iyengaryoga.be/en/find-a-teacher"

var iyengarBEPostcodeRe = regexp.MustCompile(`^(?:[A-Za-z]-)?\d{4,5}$`)

func init() {
	register(&Source{
		Slug: "iyengar-be", Name: "Belgian Iyengar Yoga Association teachers", Host: "iyengaryoga.be",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: iyengarBEList,
	})
}

func iyengarBEList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	doc, _, err := getDoc(ctx, f, iyengarBEURL)
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	for _, r := range iyengarBEBlocks(doc) {
		if seen[r.ID] {
			continue
		}
		seen[r.ID] = true
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}

// iyengarBEBlocks parses the teacher blocks of the page.
func iyengarBEBlocks(doc *goquery.Document) []Record {
	var out []Record
	taken := map[string]bool{}
	doc.Find("div.teachers-blocks div.teacher-bloc").Each(func(_ int, b *goquery.Selection) {
		name := clean(b.Find("strong").First().Text())
		if name == "" { // no name: not an entry
			return
		}
		r := Record{Name: name, Meta: map[string][]string{}}

		// the email is the text left once the decoy spans, links, list and name are gone
		txt := b.Clone()
		txt.Find("span.blockspam, a, ul, strong").Remove()
		r.Emails = structuredEmails(txt.Text())

		b.ChildrenFiltered("a[href]").Each(func(_ int, a *goquery.Selection) {
			href := strings.TrimSpace(a.AttrOr("href", ""))
			switch low := strings.ToLower(href); {
			case strings.HasPrefix(low, "tel:"):
				r.Phones = addString(r.Phones, strings.TrimSpace(href[len("tel:"):]))
			case strings.HasPrefix(low, "http"):
				r.Websites = addString(r.Websites, href)
			}
		})

		first := ""
		b.Find("ul li").Each(func(_ int, li *goquery.Selection) {
			maps := li.Find(`a[href*="google.com/maps/place/"]`).First()
			if maps.Length() == 0 {
				return
			}
			// the studio / school name is the item's text outside the link
			rest := li.Clone()
			rest.Find("a").Remove()
			if studio := clean(rest.Text()); studio != "" {
				addMeta(r.Meta, "studios", studio)
				if r.Business == "" {
					r.Business = studio
				}
			}
			if first != "" {
				return
			}
			href := maps.AttrOr("href", "")
			first = href
			r.Address, r.PostalCode, r.City, r.Country = iyengarBEPlace(href)
		})
		if r.Country == "" {
			r.Country = "Belgium" // the national association; no address to say otherwise
		}

		r.ID = yogaSlugID(name)
		if r.ID == "" || taken[r.ID] {
			sum := sha1.Sum([]byte(name + "|" + first))
			r.ID = strings.Trim(r.ID+"-"+hex.EncodeToString(sum[:])[:6], "-")
		}
		taken[r.ID] = true
		out = append(out, r)
	})
	return out
}

// iyengarBEPlace decodes a Google Maps place link: its path is the query
// "street[, number], postcode, city, country".
func iyengarBEPlace(href string) (street, postcode, city, country string) {
	i := strings.Index(href, "/maps/place/")
	if i < 0 {
		return
	}
	q := href[i+len("/maps/place/"):]
	if j := strings.IndexAny(q, "/?"); j >= 0 {
		q = q[:j]
	}
	q, err := url.QueryUnescape(q)
	if err != nil {
		return
	}
	var parts []string
	for _, p := range strings.Split(q, ",") {
		if p = clean(p); p != "" {
			parts = append(parts, p)
		}
	}
	if len(parts) < 3 {
		return clean(q), "", "", ""
	}
	country, city = parts[len(parts)-1], parts[len(parts)-2]
	rest := parts[:len(parts)-2]
	if n := len(rest); n > 0 && iyengarBEPostcodeRe.MatchString(rest[n-1]) {
		postcode, rest = rest[n-1], rest[:n-1]
	}
	return strings.Join(rest, ", "), postcode, city, country
}
