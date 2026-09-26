package directory

import (
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// TripAdvisorExtractor extracts business listings from TripAdvisor.
//
// NOTE (correctness): TripAdvisor serves us a DataDome 403 on every fetch
// method as of 2026-09-25, so no real TripAdvisor page has ever been parsed by
// this code. The HTML selectors below are modeled on TripAdvisor's documented
// structure and are covered only by synthetic fixtures. Only Listing.URLs that
// point at the business's OWN website are ever queued for enrichment — never
// a tripadvisor.com URL — mirroring the YellowPages pattern from #52.
type TripAdvisorExtractor struct{}

func (e *TripAdvisorExtractor) Name() string { return "tripadvisor" }

func (e *TripAdvisorExtractor) Match(domain string) bool {
	return domain == "tripadvisor.com" || strings.HasSuffix(domain, ".tripadvisor.com")
}

func (e *TripAdvisorExtractor) Extract(body []byte) []Listing {
	var listings []Listing
	resp := &foxhound.Response{Body: body}

	// JSON-LD — TripAdvisor embeds structured data.
	jsonlds, _ := parse.ExtractJSONLD(resp)
	for _, ld := range jsonlds {
		listing := extractFromJSONLD(ld, "tripadvisor")
		if listing.Name != "" {
			listing.URL = businessWebsite(listing.URL, "tripadvisor.com")
			listings = append(listings, listing)
		}
	}

	// HTML fallback.
	doc, err := parse.NewDocument(resp)
	if err != nil {
		return listings
	}

	seen := make(map[string]bool)
	doc.Each("[data-test-target='restaurants-list'] > div, .result-card, .listing", func(_ int, s *goquery.Selection) {
		name := strings.TrimSpace(s.Find("a[class*='name'], .result-title, .listing-title").First().Text())
		if name == "" || seen[name] {
			return
		}
		seen[name] = true

		// Prefer the business's own website link. TripAdvisor cards show an
		// off-site "Website" link alongside the internal review link.
		website := ""
		if href, ok := s.Find("a[href*='website'], a[aria-label*='Website']").First().Attr("href"); ok {
			website = absURL(href, "https://www.tripadvisor.com")
		}

		address := strings.TrimSpace(s.Find("[class*='address']").First().Text())
		rating := strings.TrimSpace(s.Find("[class*='bubble'], [aria-label*='bubbles']").First().AttrOr("aria-label", ""))
		category := strings.TrimSpace(s.Find("[class*='cuisine'], [class*='category']").First().Text())
		phone := strings.TrimSpace(s.Find("[class*='phone']").First().Text())

		listings = append(listings, Listing{
			Name:     name,
			URL:      businessWebsite(website, "tripadvisor.com"),
			Phone:    phone,
			Address:  address,
			Category: category,
			Rating:   rating,
			Source:   "tripadvisor",
		})
	})

	return listings
}
