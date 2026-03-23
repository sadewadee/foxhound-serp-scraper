package directory

import (
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// ClassPassExtractor extracts fitness studio listings from ClassPass pages.
type ClassPassExtractor struct{}

func (e *ClassPassExtractor) Name() string { return "classpass" }

func (e *ClassPassExtractor) Match(domain string) bool {
	return domain == "classpass.com" || strings.HasSuffix(domain, ".classpass.com")
}

func (e *ClassPassExtractor) Extract(body []byte) []Listing {
	var listings []Listing

	// Try JSON-LD first (ClassPass sometimes embeds structured data).
	resp := &foxhound.Response{Body: body}
	jsonlds, _ := parse.ExtractJSONLD(resp)
	for _, ld := range jsonlds {
		listing := extractFromJSONLD(ld, "classpass")
		if listing.Name != "" {
			listings = append(listings, listing)
		}
	}

	// HTML fallback — ClassPass studio cards.
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return listings
	}

	// Studio listing cards.
	selectors := []string{
		"[data-testid='studio-card']",
		".studio-card",
		"a[href*='/studios/']",
	}

	seen := make(map[string]bool)
	for _, sel := range selectors {
		doc.Each(sel, func(_ int, s *goquery.Selection) {
			name := strings.TrimSpace(s.Find("h3, h2, .studio-name, [class*='name']").First().Text())
			if name == "" || seen[name] {
				return
			}
			seen[name] = true

			href, _ := s.Attr("href")
			if href == "" {
				href, _ = s.Find("a[href]").First().Attr("href")
			}
			if href != "" && !strings.HasPrefix(href, "http") {
				href = "https://classpass.com" + href
			}

			address := strings.TrimSpace(s.Find("[class*='address'], [class*='location']").First().Text())
			category := strings.TrimSpace(s.Find("[class*='category'], [class*='activity']").First().Text())
			rating := strings.TrimSpace(s.Find("[class*='rating']").First().Text())

			listings = append(listings, Listing{
				Name:     name,
				URL:      href,
				Address:  address,
				Category: category,
				Rating:   rating,
				Source:   "classpass",
			})
		})
	}

	return listings
}
