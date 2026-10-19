package prodir

// UESCA (USA Endurance Sports Coaching Association) coach directory,
// https://uesca.com/coach-directory/ (~1,070 endurance coaches, individuals).
//
// The directory is a GravityView list: /coach-directory/?pagenum=N, 48 cards
// per page (~23 pages); a page past the end is HTTP 200 with no cards, which
// ends the walk. A card carries the name, a bio excerpt, the sports coached,
// specialization level, experience and city / state / country (free-text
// country names). The id is the Gravity Forms entry id. The profile
// (/coach-directory/entry/{id}/) adds the phone (tel: link, not on every
// profile), the website, Facebook / Instagram / LinkedIn links, credentials and
// the full bio. There is no email.
//
// Category: UESCA coaches several endurance sports, ticked as "Sport(s)
// Coached" on the card, so the record category is picked from them (see
// uescaCategory); all of them are kept in meta "sports".

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const uescaBase = "https://uesca.com/coach-directory/"

func init() {
	register(&Source{
		Slug: "uesca", Name: "UESCA coach directory", Host: "uesca.com",
		Category: "sports_running", ListingType: "individual", MinInterval: time.Second,
		List: uescaList, Detail: uescaDetail,
	})
}

var uescaIDRe = regexp.MustCompile(`/entry/(\d+)/?`)

// gvField is the GravityView field div "gv-field-11-<n>" (n may contain a dot,
// as in the address sub-fields "28.3").
func gvField(s *goquery.Selection, n string) *goquery.Selection {
	return s.Find("div.gv-field-11-" + strings.ReplaceAll(n, ".", `\.`)).First()
}

// gvText is the field's value text without its label.
func gvText(s *goquery.Selection, n string) string {
	f := gvField(s, n).Clone()
	f.Find(".gv-field-label").Remove()
	return clean(f.Text())
}

// gvItems are the bullet values of a multi-value field.
func gvItems(s *goquery.Selection, n string) []string {
	var out []string
	gvField(s, n).Find("li").Each(func(_ int, li *goquery.Selection) {
		if v := clean(li.Text()); v != "" {
			out = append(out, v)
		}
	})
	return out
}

func uescaList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	seen := map[string]bool{}
	for page := 1; page <= 200; page++ {
		doc, base, err := getDoc(ctx, f, fmt.Sprintf("%s?pagenum=%d", uescaBase, page))
		if err != nil {
			return err
		}
		cards := doc.Find("div.gv-list-view")
		if cards.Length() == 0 {
			return nil
		}
		var recs []Record
		cards.Each(func(_ int, card *goquery.Selection) {
			link := card.Find(`h3.gv-field-11-1 a, a[href*="/entry/"]`).First()
			href := absURL(base, link.AttrOr("href", ""))
			m := uescaIDRe.FindStringSubmatch(href)
			name := clean(card.Find("h3.gv-field-11-1").First().Text())
			// a card without entry link or name is not a coach
			if m == nil || name == "" || seen[m[1]] {
				return
			}
			seen[m[1]] = true
			r := Record{
				ID: m[1], URL: href, Name: name, Bio: gvText(card, "26"),
				City: gvText(card, "28.3"), Region: gvText(card, "28.4"), Country: gvText(card, "28.6"),
				Meta: map[string][]string{},
			}
			sports := gvItems(card, "8")
			r.Category = uescaCategory(sports)
			addMeta(r.Meta, "sports", sports...)
			addMeta(r.Meta, "specialization_level", gvItems(card, "7")...)
			addMeta(r.Meta, "experience", gvText(card, "5"))
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

// uescaCategory picks the record category from the sports coached: running
// (also the source default) first, then triathlon, cycling and swimming; "" (the
// default) when none of them is ticked.
func uescaCategory(sports []string) string {
	has := func(sub string) bool {
		for _, s := range sports {
			if containsFold(s, sub) {
				return true
			}
		}
		return false
	}
	switch {
	case has("running"):
		return "sports_running"
	case has("triathlon"):
		return "sports_triathlon"
	case has("cycling"):
		return "sports_cycling"
	case has("swimming"):
		return "swimming_coach"
	}
	return ""
}

func uescaDetail(ctx context.Context, f Fetcher, r Record) (Record, error) {
	doc, base, err := getDoc(ctx, f, r.URL)
	if err != nil {
		return Record{}, err
	}
	root := doc.Selection
	if root.Find("h3.gv-field-11-1").Length() == 0 {
		return Record{}, fmt.Errorf("no coach profile on %s (layout changed?)", base)
	}
	d := Record{Social: map[string]string{}, Meta: map[string][]string{}}
	if tel := gvField(root, "3").Find(`a[href^="tel:"]`).First(); tel.Length() > 0 {
		d.Phones = []string{clean(tel.Text())}
	}
	if w := absURL(base, gvField(root, "21").Find("a[href]").First().AttrOr("href", "")); strings.HasPrefix(w, "http") {
		d.Websites = []string{w}
	}
	for plat, n := range map[string]string{"facebook": "22", "instagram": "23", "linkedin": "24"} {
		if u := absURL(base, gvField(root, n).Find("a[href]").First().AttrOr("href", "")); strings.HasPrefix(u, "http") {
			d.Social[plat] = u
		}
	}
	d.Bio = clean(gvText(root, "13") + " " + gvText(root, "14"))
	addMeta(d.Meta, "credential", gvItems(root, "20")...)
	addMeta(d.Meta, "other_specialties", gvItems(root, "9")...)
	return d, nil
}
