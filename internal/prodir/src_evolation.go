package prodir

// Evolation Yoga affiliate studios, https://evolationyoga.com/studios
// (~24 affiliate cards, listing_type studio; the page says "40+ affiliates").
//
// A Squarespace page of image cards. Each card is an image wrapped in a link to
// the affiliate's own website (a few cards link to an Instagram / Facebook page
// only). One request: the ?format=json variant (155 KB, the HTML page is 1.5 MB
// of inline CSS and JS) whose "mainContent" holds the cards as HTML. The names
// are only inside the card images, so the name is the website's host.
//
// Contact fields: ONLY the website (or the social profile of a social-only
// card). No address, phone or email, and no country: the "United States"
// section also holds Canadian studios, so the section is stored as Meta
// "section" instead. The value of this source is seeding domains for enrich.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

const evolationURL = "https://evolationyoga.com/studios?format=json"

func init() {
	register(&Source{
		Slug: "evolation", Name: "Evolation Yoga affiliate studios", Host: "evolationyoga.com",
		Category: "hot_yoga_teacher", ListingType: "studio", MinInterval: time.Second,
		List: evolationList,
	})
}

var evoDashRe = regexp.MustCompile(`[^a-z0-9]+`)

func evoID(s string) string {
	return strings.Trim(evoDashRe.ReplaceAllString(strings.ToLower(s), "-"), "-")
}

func evolationList(ctx context.Context, f Fetcher, emit func(Record) error) error {
	p, err := f.Get(ctx, evolationURL, nil)
	if err != nil {
		return err
	}
	if err := p.Err(); err != nil {
		return err
	}
	var page struct {
		MainContent string `json:"mainContent"`
	}
	if err := json.Unmarshal(p.Body, &page); err != nil {
		return fmt.Errorf("evolation: %w", err)
	}
	doc, err := docFromBody([]byte(page.MainContent))
	if err != nil {
		return err
	}

	// Cards and the "INTERNATIONAL" heading in document order: every card after
	// the heading is an international one.
	section := "united_states"
	seen := map[string]bool{}
	var out []Record
	doc.Find("h1, h2, h3, h4, a.sqs-block-image-link").Each(func(_ int, s *goquery.Selection) {
		if !s.Is("a") {
			if strings.Contains(strings.ToLower(s.Text()), "international") {
				section = "international"
			}
			return
		}
		href := strings.TrimSpace(s.AttrOr("href", ""))
		u, err := url.Parse(href)
		if err != nil || u.Hostname() == "" || (u.Scheme != "http" && u.Scheme != "https") {
			return
		}
		r := Record{Meta: map[string][]string{}}
		if plat := socialPlatform(href); plat != "" {
			// a social-only card: the profile is all there is
			handle := strings.Trim(u.Path, "/")
			if !socialProfile(href) || handle == "" {
				return
			}
			r.ID, r.Name = evoID(plat+"-"+handle), handle
			r.Social = map[string]string{plat: u.Scheme + "://" + u.Host + u.Path}
		} else {
			host := strings.TrimPrefix(strings.ToLower(u.Hostname()), "www.")
			r.ID, r.Name = evoID(host), host
			r.Websites = []string{u.Scheme + "://" + u.Host + strings.TrimRight(u.Path, "/")}
		}
		if r.ID == "" || seen[r.ID] {
			return
		}
		seen[r.ID] = true
		r.URL = "https://evolationyoga.com/studios"
		addMeta(r.Meta, "section", section)
		out = append(out, r)
	})
	for _, r := range out {
		if err := emit(r); err != nil {
			return err
		}
	}
	return nil
}
