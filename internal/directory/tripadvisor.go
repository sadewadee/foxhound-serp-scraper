package directory

import (
	"strings"

	"github.com/PuerkitoBio/goquery"

	foxhound "github.com/sadewadee/foxhound"
	"github.com/sadewadee/foxhound/parse"
)

// TripAdvisorExtractor extracts business listings from TripAdvisor.
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

		href, _ := s.Find("a[href*='/Restaurant_Review'], a[href*='/Attraction_Review'], a[href*='/Hotel_Review']").First().Attr("href")
		if href != "" && !strings.HasPrefix(href, "http") {
			href = "https://www.tripadvisor.com" + href
		}

		address := strings.TrimSpace(s.Find("[class*='address']").First().Text())
		rating := strings.TrimSpace(s.Find("[class*='bubble'], [aria-label*='bubbles']").First().AttrOr("aria-label", ""))
		category := strings.TrimSpace(s.Find("[class*='cuisine'], [class*='category']").First().Text())

		listings = append(listings, Listing{
			Name:     name,
			URL:      href,
			Address:  address,
			Category: category,
			Rating:   rating,
			Source:   "tripadvisor",
		})
	})

	return listings
}
