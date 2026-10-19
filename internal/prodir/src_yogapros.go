package prodir

// Yoga Pros teacher directory, https://member.theyogapros.com/teachers
// (~6,175 teachers, individuals).
//
// The list is a server-rendered grid, 15 cards per page
// (/teachers?page=N&sized=0, ~412 pages; the page carries an "N results"
// counter). It is the only way in (no sitemap, no API), although robots.txt
// disallows the /teachers? paging path: crawl it only knowingly. A card gives
// the id, name, level label, studio, "City, Country" and the website / social
// icons. Email and phone are login-gated, so there are none; Bio is empty and
// there is no Detail (a profile only adds styles and qualification hours, for
// ~6k more requests).
//
// Per-record override: a teacher listed under the Hot Yoga style filter
// (?yoga_style=13, ~113 teachers, ~8 pages) is category hot_yoga_teacher. The
// card does not show the style, so List first walks that filtered list to
// collect the ids, then walks the full list.

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const (
	yogaprosBase     = "https://member.theyogapros.com"
	yogaprosPerPage  = 15
	yogaprosHotStyle = "yoga_style=13"
)

var yogaprosTotalRe = regexp.MustCompile(`([\d,]+)\s+results?\b`)

func init() {
	register(&Source{
		Slug: "yogapros", Name: "Yoga Pros teacher directory", Host: "member.theyogapros.com",
		Category: "yoga_teacher", ListingType: "individual", MinInterval: time.Second,
		List: yogaprosList,
	})
}

// yogaprosWalk pages through /teachers (narrowed by query, "" or "k=v&") and
// calls fn with the cards of each page. It ends at the first empty page or
// after the last page the result counter announces.
func yogaprosWalk(ctx context.Context, f Fetcher, query string, fn func([]Record) error) error {
	pages := 0 // unknown until page 1 shows the counter
	for page := 1; page <= 1000; page++ {
		p, err := f.Get(ctx, fmt.Sprintf("%s/teachers?%spage=%d&sized=0", yogaprosBase, query, page), nil)
		if err != nil {
			return err
		}
		if err := p.Err(); err != nil {
			return err
		}
		if page == 1 {
			if m := yogaprosTotalRe.FindSubmatch(p.Body); m != nil {
				n, _ := strconv.Atoi(strings.ReplaceAll(string(m[1]), ",", ""))
				pages = (n + yogaprosPerPage - 1) / yogaprosPerPage
			}
		}
		doc, err := docFromBody(p.Body)
		if err != nil {
			return err
		}
		recs := yogaprosCards(doc, p.URL)
		if len(recs) == 0 {
			return nil
		}
		if err := fn(recs); err != nil {
			return err
		}
		if pages > 0 && page >= pages {
			return nil
		}
	}
	return nil
}

func yogaprosList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	hot := map[string]bool{}
	err := yogaprosWalk(ctx, f, yogaprosHotStyle+"&", func(recs []Record) error {
		for _, r := range recs {
			hot[r.ID] = true
		}
		return nil
	})
	if err != nil {
		return err
	}
	seen := map[string]bool{}
	return yogaprosWalk(ctx, f, "", func(recs []Record) error {
		for _, r := range recs {
			if seen[r.ID] {
				continue
			}
			seen[r.ID] = true
			if hot[r.ID] {
				r.Category = "hot_yoga_teacher"
			}
			if err := emit(r); err != nil {
				return err
			}
		}
		return nil
	})
}

// yogaprosSocial are the data-social-media values that go to Record.Social.
var yogaprosSocial = map[string]bool{
	"facebook": true, "instagram": true, "youtube": true, "linkedin": true, "tiktok": true, "twitter": true,
}

// yogaprosCards parses the teacher cards of one listing page.
func yogaprosCards(doc *goquery.Document, base string) []Record {
	var out []Record
	doc.Find("div.teacher-container[data-teacher-id]").Each(func(_ int, card *goquery.Selection) {
		id := strings.TrimSpace(card.AttrOr("data-teacher-id", ""))
		a := card.Find(".h3 a").First()
		name := clean(a.Text())
		if id == "" || name == "" { // a card without id or name is not an entry
			return
		}
		r := Record{
			ID: id, URL: absURL(base, a.AttrOr("href", "")), Name: name,
			Business: clean(card.Find(`a[href^="business/"]`).First().Text()),
			Meta:     map[string][]string{},
		}
		addMeta(r.Meta, "level", card.Find(".exp-loc > div").First().Text())
		r.City, r.Country = yogaSplitLocation(card.Find("i.fa-location-dot").First().Parent().Text())
		card.Find("a[data-social-media]").Each(func(_ int, s *goquery.Selection) {
			href := strings.TrimSpace(s.AttrOr("href", ""))
			if !strings.HasPrefix(strings.ToLower(href), "http") {
				return
			}
			switch plat := strings.ToLower(s.AttrOr("data-social-media", "")); {
			case plat == "website" || plat == "blog": // Websites[0] is the own site; a blog follows it
				r.Websites = addString(r.Websites, href)
			case yogaprosSocial[plat]:
				if r.Social == nil {
					r.Social = map[string]string{}
				}
				if r.Social[plat] == "" {
					r.Social[plat] = href
				}
			}
		})
		out = append(out, r)
	})
	return out
}
